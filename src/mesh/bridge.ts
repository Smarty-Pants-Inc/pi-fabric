import { copyFabricPrincipal, type FabricPrincipal } from "../fabric-provenance.js";
import { createHash } from "node:crypto";
import { isMeshIncarnationPending, isMeshSnapshotRetriesExhausted, isMeshLockTimeout,
  MESH_INCARNATION_PENDING_CODE, MESH_SNAPSHOT_RETRIES_EXHAUSTED_CODE, MESH_LOCK_TIMEOUT_CODE, meshDeferredSummary } from "../core/atomic-write.js";
import { performance as monotonicClock } from "node:perf_hooks";
import fs from "node:fs";
import path from "node:path";
import type { Readable, Writable } from "node:stream";
import { ownProcessIncarnation, readPhysicalHostIdentity, readFileRetrying, writeFileAtomic, writeJsonAtomic } from "../core/atomic-write.js";
import { type BridgeLeaseProcess, type FabricHostLease, type FabricMirrorLease, MIRROR_FRESH_MS, hostLeaseCacheGeneration, hostLeaseGeneration, hostLeaseExpiry, hostLeasePath, hostLiveness, readHostLease, readHostLeases, removeHostLeaseIf, STATE_LEASE_RENEW_MS, writeHostLease, writeHostLeaseAsync } from "../topology/host-leases.js";
import { meshDirectoryStamp } from "../topology/publication-generation.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../topology/types.js";
import { ROOT_ID_PREFIX } from "../topology/root-inbox-shared.js";
import { participantFilePresent, readParticipantFiles } from "../topology/participant-files.js";
import { FABRIC_MESH_OUTAGE_ALARM_TOPIC, isMeshAlarmTarget, meshCursorGeneration, type MeshBatchOperation, type MeshBatchView, type MeshEvent, type MeshIdentity, type MeshPublishInput, type MeshStateEntry, type MeshStore } from "./store.js";
import { CommitOutbox, type CommitOutboxEffect, type CommitOutboxPlan } from "./commit-outbox.js";
import { assertBridgeStateBackend, isMeshRetryableBusy, isMeshStateWalCap } from "./state-backend.js";
import { BridgeDeliveryAgeMonitor, type BridgeDeliveryAgeAlarm } from "./bridge-delivery-age.js";
import { watchBridgeStore, type BridgeChange } from "./change-notifier.js";
import { BRIDGE_BATCH_MAX_EVENTS } from "./event-log.js";
import { BridgeLegacyBaselineAlarmEpisode, supportsLegacyBaselineAlarm, type BridgeLegacyBaselineAlarm } from "./bridge-baseline-alarms.js";
import { controlBodyFingerprint, FABRIC_IDEMPOTENCY_KEY_REUSED, FabricIdempotencyKeyReusedError } from "./control-idempotency.js";
import { HUB_FACTORY_RECIPIENT, isAddressedHubFactoryEvent, isWorkTopic, parseWorkTopic } from "./bridge-topics.js";
export { HUB_FACTORY_RECIPIENT, isWorkTopic, parseWorkTopic } from "./bridge-topics.js";

/**
 * Fabric mesh bridge v1 (smarty-dev#2004). Each host keeps its own mesh; one bridge process on the
 * hub (Dev1) links it to one remote host's mesh over a stdio transport (ssh to a forced command
 * that runs `mesh-bridge agent`). The bridge republishes allow-listed mesh events and mirrors
 * each side's live root presence. Cross-spoke commands use delivery-only hub-link IPC rather
 * than a hub body log; the destination reuses its own existing stdio link and mesh store.
 *
 * Trust: the hub side is trusted; the remote side is not. An event from the remote crosses only
 * when its sender is a live participant native to the remote (and no hub record uses its id), and
 * its recipient is native to the hub, a canonical root held on another link (control/ACK only),
 * or the hub factory's role address `factory` on a work event or owner wake (smarty-dev#9352).
 * The trusted hub re-exports other links' roots and carries their controls one final hop.
 * Unaddressed WAIT/ETA issue records also cross to the factory (smarty-dev#9349), not other
 * broadcasts. Mirrored records never replace a native record. The one
 * exception to "no hub record uses its id" is a hub-written handoff (smarty-dev#9074, below).
 */

/** v2 requires change notifications; v1 remains the explicit pre-capability rollout path. */
export const BRIDGE_PROTOCOL_VERSION = 2;
const LEGACY_BRIDGE_PROTOCOL_VERSION = 1;
const HOST_PREFIX = "topology/hosts/";
const PARTICIPANT_PREFIX = "topology/participants/";
/** Freshness bound for an observation from one physical bridge peer. */
export const BRIDGE_LEASE_MS = MIRROR_FRESH_MS;
const DEFAULT_PRESENCE_MS = 5_000;
/** A retained source event revalidates after this delay; this is NOT an event/authority TTL. */
const DEFAULT_MEMBERSHIP_RETRY_MS = 1_000;
/** A missed change notification is only a safety net: its retry backs off to this bound. */
const MEMBERSHIP_RETRY_MAX_MS = 30_000;
/** Transitional mixed-version exception only: remove after all fleet agents support changes. */
const LEGACY_POLL_MS = 250;
// Keep health sampling independent of caller fake timers and wall-clock adjustments.
const setWatchdogTimeout = setTimeout;
const clearWatchdogTimeout = clearTimeout;
/** Commands whose ACK this link relays back to a non-participant hub sender (smarty-dev#9751). */
const MAX_AWAITING_ACKS = 4_096;
type Direction = "toRemote" | "toLocal";
type DirectionWakeReason = "real" | "safety" | "grace" | "sync" | "other";
export const BRIDGE_WATCHDOG_INTERVAL_MS = 15_000;
export const BRIDGE_WATCHDOG_PROBE_TIMEOUT_MS = 5_000;
export const BRIDGE_WATCHDOG_STALL_AFTER_MS = 30_000;
export const BRIDGE_DIRECTION_STALL_AFTER_MS = 180_000;
export const BRIDGE_STALL_ALARM_TOPIC = "fabric.alarm.bridge-stall";

export interface BridgeStallAlarm {
  v: 1;
  host: string;
  peer: string;
  direction: Direction | "both";
  lastOkToRemote: string | null;
  lastOkToLocal: string | null;
  stallId: string;
  reason: string;
  at: string;
}

let lastBridgeStallId = 0n;
const nextBridgeStallId = (): string => {
  const now = process.hrtime.bigint();
  lastBridgeStallId = now > lastBridgeStallId ? now : lastBridgeStallId + 1n;
  return lastBridgeStallId.toString();
};

/** Stable operator handoff alongside the link cursor, normalized to an absolute per-peer path. */
export const bridgeStallReasonPath = (cursorPath: string): string => `${path.resolve(cursorPath)}.last-stall-reason`;
/** How long a carried command's ACK may still be relayed back (smarty-dev#9751). */
const AWAITING_ACK_MS = 15 * 60_000;
/** A relayed ACK goes back only to one session or factory host: never an org, topic or broadcast name. */
const UNICAST_REPLY_TO = /^(?:session|factory-host):[A-Za-z0-9._-]{1,128}$/;
/** The outcome fields a relayed ACK carries (no result payload). */
const RELAYED_ACK_FIELDS = new Set(["version", "commandId", "targetId", "ownerIncarnation", "accepted", "messageId", "error", "errorCode", "notRun"]);
/** A bridge call the remote has not answered by then closes the transport. */
export const DEFAULT_CALL_TIMEOUT_MS = 30_000;
/** stop() waits at most this long for the loop and in-flight directions before closing transport. */
const DEFAULT_STOP_MS = 5_000;
const LOCK_RETRY_MIN_MS = 100;
const LOCK_RETRY_MAX_MS = 2_000;
const BRIDGE_STARTUP_RETRY_BUDGET_MS = 3 * 60_000;
const MIRROR_BATCH_MAX_OPS = 64;
const MIRROR_BATCH_MAX_EFFECTS = 32;
// A WAL-cap refusal (FABRIC_MESH_STATE_WAL_CAP) can last until the operator rolls back: log it at most once a minute.
const WAL_CAP_LOG_MS = 60_000;
// A capped WAL stays capped until a reader lets go or the operator rolls back: back off 1 s doubling to 30 s,
// never the 100 ms lock-retry cadence that would hammer a stuck WAL (pi-fabric#694 P1 2).
const WAL_CAP_RETRY_MIN_MS = 1_000;
const WAL_CAP_RETRY_MAX_MS = 30_000;
/** The next wait after a WAL-cap refusal: `min` first, then doubling, at most 30 s. */
export const walCapRetryDelay = (previous: number | undefined, min = WAL_CAP_RETRY_MIN_MS): number =>
  previous === undefined ? min : Math.min(Math.max(previous * 2, min), WAL_CAP_RETRY_MAX_MS);
const meshBusyLabel = (error: unknown): string =>
  isMeshIncarnationPending(error) ? "mesh incarnation pending"
    : isMeshSnapshotRetriesExhausted(error) ? "mesh snapshot retries exhausted"
      : isMeshStateWalCap(error) ? "mesh state WAL cap" : "mesh lock timeout";
const MAX_RPC_LINE_BYTES = 16 * 1024 * 1024;
/** A read page's event bytes stay under this, well inside one frame with its JSON envelope. */
export const BRIDGE_PAGE_BYTES = 8 * 1024 * 1024;
const NAME_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$/;
const RELAY_UNAVAILABLE_CODE = "FABRIC_RELAY_DESTINATION_UNAVAILABLE";
const RELAY_RECEIPT_PREFIX = "bridge-relay-receipt:";
const relayUnavailable = (): Error & { code: string; retryable: true } =>
  Object.assign(new Error("Relay destination unavailable"), { code: RELAY_UNAVAILABLE_CODE, retryable: true as const });

/** A host-only receipt has no payload or bridge stamp and must never be forwarded as a command. */
const isRelayReceipt = (event: Pick<MeshEvent, "id" | "dedupeKey" | "verification">): boolean =>
  event.verification === "bridge" && ((typeof event.id === "string" && event.id.startsWith(RELAY_RECEIPT_PREFIX)) ||
    (typeof event.dedupeKey === "string" && event.dedupeKey.startsWith(RELAY_RECEIPT_PREFIX)));

const keyFor = (prefix: string, id: string): string => prefix + createHash("sha256").update(id).digest("hex");

// Natives' participant files (smarty-dev#2004): their entries, and whether a key has a file at all,
// readable or not (fail closed, security pass S3 on #142).
const participantFileSnapshot = (root: string): { entries: readonly MeshStateEntry[]; present: (key: string) => boolean } => {
  const entries = readParticipantFiles(root, { maxAgeMs: 0 });
  let names: Set<string>;
  try {
    names = new Set(fs.readdirSync(path.join(root, "participants")));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") return { entries, present: (key) => participantFilePresent(root, key) };
    names = new Set();
  }
  return { entries, present: (key) => key.startsWith(PARTICIPANT_PREFIX) && names.has(`${key.slice(PARTICIPANT_PREFIX.length)}.json`) };
};

interface ParticipantDirectoryWatch {
  watcher: fs.FSWatcher | undefined;
  rootWatcher: fs.FSWatcher | undefined;
  generation: number;
}
const participantDirectoryWatches = new Map<string, ParticipantDirectoryWatch>();
const closeParticipantDirectoryWatches = (): void => {
  for (const state of participantDirectoryWatches.values()) {
    try { state.watcher?.close(); } catch { /* process shutdown */ }
    try { state.rootWatcher?.close(); } catch { /* process shutdown */ }
    state.watcher = undefined;
    state.rootWatcher = undefined;
  }
};
let participantWatchExitHookInstalled = false;
const ensureParticipantDirectoryWatch = (root: string): number => {
  let state = participantDirectoryWatches.get(root);
  if (!state) {
    state = { watcher: undefined, rootWatcher: undefined, generation: 0 };
    participantDirectoryWatches.set(root, state);
  }
  const changed = (): void => { state!.generation += 1; };
  if (!participantWatchExitHookInstalled) {
    process.once("exit", closeParticipantDirectoryWatches);
    participantWatchExitHookInstalled = true;
  }
  const directory = path.join(root, "participants");
  if (!state.watcher) {
    try {
      const watcher = fs.watch(directory, { persistent: false }, (_event, filename) => {
        if (state!.watcher !== watcher) return;
        if (filename === null || filename.toString() === "" || filename.toString().endsWith(".json")) changed();
      });
      state.watcher = watcher;
      watcher.on("error", () => { if (state!.watcher === watcher) { state!.watcher = undefined; changed(); } });
      watcher.on("close", () => { if (state!.watcher === watcher) { state!.watcher = undefined; changed(); } });
    } catch {
      // A missing directory is watched at its parent until the first participant arrives.
      if (!state.rootWatcher) {
        try {
          const watcher = fs.watch(root, { persistent: false }, (_event, filename) => {
            if (state!.rootWatcher === watcher && filename?.toString() === "participants") {
              state!.rootWatcher = undefined;
              changed();
              try { watcher.close(); } catch { /* re-arm on the next snapshot */ }
            }
          });
          state.rootWatcher = watcher;
          watcher.on("error", () => { if (state!.rootWatcher === watcher) { state!.rootWatcher = undefined; changed(); } });
          watcher.on("close", () => { if (state!.rootWatcher === watcher) { state!.rootWatcher = undefined; changed(); } });
        } catch { changed(); }
      }
    }
  }
  return state.generation;
};
const participantDirectoryGeneration = (root: string): number => participantDirectoryWatches.get(root)?.generation ?? -1;

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isIdempotencyKeyReused = (error: unknown): boolean =>
  isObject(error) && error.code === FABRIC_IDEMPOTENCY_KEY_REUSED;

const isIdentity = (value: unknown): value is MeshIdentity =>
  isObject(value) && typeof value.id === "string" && value.id.length > 0 && typeof value.name === "string" &&
  (value.kind === "main" || value.kind === "actor" || value.kind === "agent");

export const validBridgeName = (name: string): boolean => NAME_PATTERN.test(name);

/** v1 allow-list: control commands and acks, work events, owner wakes, and session-addressed bridge/outage alarms. */
export const isBridgedTopic = (event: Pick<MeshEvent, "topic" | "kind"> & { to?: unknown }): boolean =>
  event.topic === "fabric.control.command" ||
  event.topic === "fabric.control.ack" ||
  isWorkTopic(event.topic) ||
  (event.topic === "ops.owner" && (event.kind === "pr.wake" || event.kind === "inbox.age.alarm")) ||
  (event.topic === "ops.owner" && (event.kind === "bridge.lease.alarm" || event.kind === "bridge.lease.recovered" ||
    event.kind === "bridge.delivery-age.alarm" || event.kind === "bridge.delivery-age.recovered" ||
    event.kind === "bridge.legacy-baseline.alarm" || event.kind === "bridge.legacy-baseline.recovered") && isMeshAlarmTarget(event.to)) ||
  (event.topic === BRIDGE_STALL_ALARM_TOPIC && event.kind === "bridge-stall" && isMeshAlarmTarget(event.to)) ||
  (event.topic === FABRIC_MESH_OUTAGE_ALARM_TOPIC && (event.kind === "mesh-outage" || event.kind === "mesh-recovered") &&
    isMeshAlarmTarget(event.to));

/**
 * Whether a remote event belongs on the hub factory's topic: literal `to: factory` on a parsed
 * work topic or allow-listed owner wake, or an unaddressed WAIT/ETA issue record (the kinds
 * setup/factory/mesh_answers.py consumes, smarty-dev#9349). Other broadcasts and explicit
 * recipients retain their existing routing; this exception is only used toward the hub.
 */
export const isHubFactoryEvent = (event: Pick<MeshEvent, "topic" | "kind" | "to">): boolean =>
  isAddressedHubFactoryEvent(event) ||
  (event.to === undefined && (event.kind === "wait" || event.kind === "eta") && parseWorkTopic(event.topic) !== undefined);

/**
 * Two per-link token buckets: `perMinute` events refill each minute, up to `burst` held, and
 * `bytesPerMinute` bytes, up to `byteBurst` held (each defaults to DEFAULT_FACTORY_RATE's).
 */
export interface FactoryRate {
  perMinute: number;
  burst: number;
  bytesPerMinute?: number;
  byteBurst?: number;
}

/**
 * The default per-link bound on the factory exception (pi-fabric#876 review, P1-2, and security
 * pass, (c)): 30 events a minute, burst 60, and 512 KiB a minute, burst 1 MiB, of the events as
 * published. Only events that cross through the factory exception (addressed to its role or
 * unaddressed WAIT/ETA issue records) spend from either bucket; hub-native recipients do not.
 */
export const DEFAULT_FACTORY_RATE: Readonly<Required<FactoryRate>> = Object.freeze({
  perMinute: 30, burst: 60, bytesPerMinute: 512 * 1024, byteBurst: 1024 * 1024,
});

/** At most one factory-rate deferral line per this interval. */
const FACTORY_RATE_LOG_MS = 60_000;

/** At most one "not addressed across" count line per this interval (smarty-dev#9352). */
const UNADDRESSED_LOG_MS = 60_000;
/** The count line names at most this many distinct topics. */
const UNADDRESSED_TOPICS = 10;

/** Origin and cursor-recovery event id; a trusted hub may add one final `via` hop. */
export const bridgeStampOf = (event: Pick<MeshEvent, "data">): { from: string; id: string; via?: string } | undefined => {
  const stamp = isObject(event.data) ? event.data.bridge : undefined;
  return isObject(stamp) && typeof stamp.from === "string" && typeof stamp.id === "string"
    ? { from: stamp.from, id: stamp.id, ...(typeof stamp.via === "string" ? { via: stamp.via } : {}) }
    : undefined;
};

const hasBridgeField = (event: Pick<MeshEvent, "data">): boolean =>
  isObject(event.data) && Object.hasOwn(event.data, "bridge");

const hasHubHop = (event: Pick<MeshEvent, "data">): boolean =>
  isObject(event.data) && isObject(event.data.bridge) && Object.hasOwn(event.data.bridge, "via");

export interface BridgeHost {
  record: FabricHostRecord;
  /** The effective lease expiry on the source (the later of state and file lease). */
  expiresAt: number;
  /** Source clock at the native lease read; absent on older bridge agents. */
  observedAt?: number;
}

/** Minimal attribution retained when admission drops malformed or conflicting presence. */
export interface BridgeDroppedPresence {
  type: "host" | "participant";
  /** False only when local admission dropped this claim because a native/reserved id wins. */
  protected?: boolean;
  id?: string;
  rootId?: string;
  identityId?: string;
  ownerHostId?: string;
  ownerIdentityId?: string;
}

export interface BridgePresence {
  hosts: BridgeHost[];
  /** Root participants of those hosts. */
  participants: FabricParticipantRecord[];
  /** Identifiers for records dropped by admission, when they can be safely attributed. */
  dropped?: BridgeDroppedPresence[];
  /** True when this snapshot is incomplete and must not be used as withdrawal evidence. */
  partial?: boolean;
  /** Number of malformed, conflicting, or otherwise dropped records observed in the raw scan. */
  droppedCount?: number;
  /**
   * Every id and identity id of native records, live or not: a peer may never claim them. Only a
   * hub handoff in force to this link waives a dead native's own ids (smarty-dev#9074).
   */
  reserved: string[];
}

type BridgeMirrorPresence = Pick<BridgePresence, "hosts" | "participants" | "dropped" | "partial" | "droppedCount">;

const validDroppedCount = (value: unknown): number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : 0;
const relevantDroppedCount = (value: unknown): number => Array.isArray(value) ? value.length : 0;
const presenceDroppedCount = (presence: BridgeMirrorPresence): number => {
  const raw: unknown = presence;
  if (!isObject(raw)) return 0;
  return Math.max(validDroppedCount(raw.droppedCount), relevantDroppedCount(raw.dropped));
};
const presenceIsPartial = (presence: BridgeMirrorPresence): boolean => {
  const raw: unknown = presence;
  return !isObject(raw) || raw.partial === true || presenceDroppedCount(presence) > 0;
};

const copyMirrorPresence = (presence: BridgeMirrorPresence): BridgeMirrorPresence =>
  JSON.parse(JSON.stringify({ hosts: presence.hosts, participants: presence.participants })) as BridgeMirrorPresence;

const LEGACY_PARTIAL_HOLD_PERIODS = 3;
const LEGACY_PARTIAL_HOLD_MS = BRIDGE_LEASE_MS * LEGACY_PARTIAL_HOLD_PERIODS;
interface LegacyMirrorSnapshot {
  /** The exact complete projection acknowledged by this legacy peer. */
  presence: BridgeMirrorPresence;
  /** First partial omission time per root; kept with the baseline so renewals cannot reset it. */
  firstHeldAtByRoot: Map<string, number>;
}
const copyLegacySnapshot = (presence: BridgeMirrorPresence, firstHeldAtByRoot = new Map<string, number>()): LegacyMirrorSnapshot =>
  ({ presence: copyMirrorPresence(presence), firstHeldAtByRoot: new Map(firstHeldAtByRoot) });

/**
 * Legacy receivers treat every mirror as withdrawal evidence. Hold omitted roots for at most three
 * lease periods from their first partial omission, regardless of later renewals. Three periods
 * (45 s) tolerates short incomplete scans while placing a hard bound on stale authority. A fresh
 * complete scan resets the per-root clock by replacing this snapshot without hold metadata.
 */
const completeLegacyPartial = (presence: BridgeMirrorPresence, previous: LegacyMirrorSnapshot | undefined,
  now: number): LegacyMirrorSnapshot => {
  const hosts = new Map(presence.hosts.filter(({ record }) => isRootAuthorityHost(record) || isNativeNonRootHost(record))
    .map(host => [host.record.id, host]));
  const participants = new Map(presence.participants.filter(isRootAuthorityParticipant).map(participant => [participant.id, participant]));
  const oldHosts = new Map((previous?.presence.hosts ?? []).map(host => [host.record.id, host]));
  const oldParticipants = new Map((previous?.presence.participants ?? []).map(participant => [participant.id, participant]));
  const roots = new Set([...oldHosts.values()].filter(host => isRootAuthorityHost(host.record)).map(host => host.record.id));
  const firstHeldAtByRoot = new Map<string, number>();
  for (const rootId of roots) {
    const oldRootHosts = [...oldHosts.values()].filter(host => host.record.rootId === rootId);
    const oldRootParticipants = [...oldParticipants.values()].filter(participant => participant.rootId === rootId);
    const omittedHosts = oldRootHosts.filter(host => !hosts.has(host.record.id));
    const omittedParticipants = oldRootParticipants.filter(participant => !participants.has(participant.id));
    if (omittedHosts.length === 0 && omittedParticipants.length === 0) continue;
    const firstHeldAt = previous?.firstHeldAtByRoot.get(rootId) ?? now;
    if (now - firstHeldAt >= LEGACY_PARTIAL_HOLD_MS) continue;
    firstHeldAtByRoot.set(rootId, firstHeldAt);
    for (const host of omittedHosts) {
      const expiresAt = now + BRIDGE_LEASE_MS;
      const { observedAt: _observedAt, ...sample } = host;
      hosts.set(host.record.id, { ...sample, record: { ...host.record, expiresAt }, expiresAt });
    }
    for (const participant of omittedParticipants) {
      const owner = hosts.get(participant.ownerHostId);
      const oldOwner = oldHosts.get(participant.ownerHostId);
      const sameOwner = owner && oldOwner && owner.record.startedAt === oldOwner.record.startedAt &&
        owner.record.incarnationToken === oldOwner.record.incarnationToken;
      if (sameOwner) participants.set(participant.id, participant);
    }
  }
  const rootIds = new Set([...hosts.values()].filter(({ record }) => isRootAuthorityHost(record)).map(({ record }) => record.id));
  return { presence: {
    hosts: [...hosts.values()].filter(({ record }) => isRootAuthorityHost(record) ||
      (isNativeNonRootHost(record) && rootIds.has(record.rootId))),
    participants: [...participants.values()].filter(participant => isRootAuthorityParticipant(participant) && rootIds.has(participant.rootId)),
  }, firstHeldAtByRoot };
};

export interface BridgeSkip { id: string; sequence: number; topic: string; bytes: number }

export interface BridgeRead {
  /** Allow-listed events after the cursor, in sequence order. */
  events: MeshEvent[];
  /** Allow-listed events too large for one frame, passed over (the bridge logs each). */
  skipped?: BridgeSkip[];
  /** The last sequence read (the cursor to resume from). */
  through: number;
  /** Generation-tagged byte offset after the whole page; absent for legacy sequence reads. */
  offset?: number;
}

export interface BridgeHead { through: number; offset?: number }

export interface BridgePublishReceipt { sequence: number; createdAt?: number }

export interface BridgePublish {
  principal?: FabricPrincipal | undefined;
  topic: string;
  kind: string;
  from: MeshIdentity;
  to: string;
  text?: string;
  data: Record<string, unknown> & { bridge: { from: string; id: string; via?: string } };
}

/** One side of the bridge: the local mesh directly, or the remote one through the agent. */
export interface BridgeSide {
  latestSequence(): Promise<number>;
  /** Remote heartbeat read with a non-fatal deadline; a missed probe suspends watchdog notifications only. */
  watchdogHead?(timeoutMs: number): Promise<number>;
  read(after: number): Promise<BridgeRead>;
  /** Install before the first snapshot. Changes are latched during in-flight work.
   * Watch failure is fatal. undefined selects transitional legacy-remote polling only with
   * reported protocolVersion 1; local stores always require watches. Manual step() needs no subscription. */
  subscribeChanges?(changed: (kind: BridgeChange) => void, failed: (error: Error) => void): (() => void) | undefined | Promise<(() => void) | undefined>;
  /** Reported hello version, required before a remote may use the pre-capability polling path. */
  readonly protocolVersion?: number | undefined;
  /** An open, negotiated traffic lane remains usable despite a retained pending mirror. */
  readonly pendingMirrorTraffic?: boolean;
  /** Optional v1 capability: old peers and sequence-only cursor files remain readable. */
  latestCursor?(): Promise<BridgeHead>;
  tail?(after: number, offset?: number): Promise<BridgeRead>;
  presence(): Promise<BridgePresence>;
  /** Hub-local only: live direct mirrors owned by other links, never re-exported mirrors. */
  relayPresence?(): Promise<Pick<BridgePresence, "hosts" | "participants">>;
  /** Direct origin of a held root on another link, for captured control destinations. */
  relayOrigin?(id: string): string | undefined;
  /** Hub-local only: a structurally attributable projection whose authority is stale; never routing authority. */
  relayStaleOrigin?(id: string): string | undefined;
  /** Publish a bridged event; with `held`, only if this link holds each of those ids at commit. */
  publish(event: BridgePublish, held?: string[]): Promise<{ sequence: number }>;
  /** Internal append receipt with the destination timestamp; older BridgeSide implementations may omit it. */
  publishWithReceipt?(event: BridgePublish, held?: string[]): Promise<BridgePublishReceipt>;
  /** Hub-local only: persist a payload-free successful relay receipt under the sender fence. */
  recordRelay?(event: BridgePublish, held?: string[]): Promise<{ sequence: number }>;
  /** Optional capability: returns a durably committed, ordered prefix (1..256 events). */
  publishBatch?(events: Array<{ event: BridgePublish; held?: string[] }>): Promise<Array<{ sequence: number }>>;
  /** Internal append receipts with destination timestamps; absent on older BridgeSide implementations. */
  publishBatchWithReceipt?(events: Array<{ event: BridgePublish; held?: string[] }>): Promise<BridgePublishReceipt[]>;
  /** Whether this link holds a live mirror answering to the id (the hub side only). */
  holds?(id: string, identity?: MeshIdentity): boolean;
  /** Whether this link has a mirror record for the id, even if its lease lapsed (hub only). */
  mirrored?(id: string): boolean;
  /**
   * Replace this side's mirror of the peer's presence. `observedAt` (this side's clock, never a
   * peer's) is when the snapshot was requested; liveness is judged then, not at commit.
   */
  mirror(presence: BridgeMirrorPresence, observedAt?: number): Promise<void>;
  /** Original ids of events the peer bridged into this side after a sequence, one bounded page. */
  bridgedIds(after: number): Promise<BridgedIds>;
  /** End the transport and fail pending calls (a remote side only). */
  close?(error?: Error): void;
  /** End remote stdin cleanly while keeping stdout readable until the peer exits. */
  closeGracefully?(error?: Error): void;
  /** The peer's records this side holds now (the hub side only): the link's current authority. */
  owned?(): Promise<Pick<BridgePresence, "hosts" | "participants">>;
  /** Fence future mirror writes without changing persisted mirrors; stop uses this before leaving. */
  fence?(): Promise<void>;
  /** Explicitly fence writes and delete this link's mirrors for operator/decommission use. */
  withdraw?(): Promise<void>;
}

export interface BridgedIds { ids: string[]; through: number; done: boolean }
/** One bridgedIds page holds at most this many ids (about 2 MiB on the wire). */
const BRIDGED_IDS_PAGE = 50_000;

const remoteHostOf = (value: unknown): string | undefined =>
  isObject(value) && typeof value.remoteHost === "string" ? value.remoteHost : undefined;

/** A re-export belongs to its immediate hub link, but routes and ACKs retain its origin. */
const mirrorLinkOf = (value: unknown): string | undefined =>
  remoteHostOf(value) !== undefined && isObject(value) && typeof value.bridgeVia === "string"
    ? value.bridgeVia : remoteHostOf(value);

// Identification is not admission. Presence must carry an identifiable malformed direct
// sample to the receiver's root-wide skip, rather than turn it into omission/withdrawal.
// Keep the unchecked timestamps intact; neither this reader nor a lease may repair them.
const identifiableHostOf = (key: string, value: unknown): FabricHostRecord | undefined => {
  if (!isObject(value) || value.format !== 1 || typeof value.id !== "string") return undefined;
  if (key !== keyFor(HOST_PREFIX, value.id) || typeof value.rootId !== "string" || !isIdentity(value.identity)) return undefined;
  return value as unknown as FabricHostRecord;
};

const finiteHostTimestamps = (record: FabricHostRecord): boolean =>
  Number.isFinite(record.startedAt) && Number.isFinite(record.updatedAt) && Number.isFinite(record.expiresAt);

const PARTICIPANT_KINDS = new Set(["root", "agent", "actor", "provider"]);
const PARTICIPANT_TRANSPORTS = new Set(["auto", "process", "sandbox", "tmux", "screen", "localterm", "herdr", "host", "hosted"]);

const carriesRemoteHost = (value: unknown): value is Record<string, unknown> =>
  isObject(value) && Object.hasOwn(value, "remoteHost") && value.remoteHost !== undefined;

const trustedRelayOf = (value: unknown, bridgeVia?: string): boolean =>
  carriesRemoteHost(value) && typeof value.remoteHost === "string" && value.remoteHost.length > 0 &&
  bridgeVia !== undefined && bridgeVia.length > 0 && value.remoteHost !== bridgeVia && value.bridgeVia === bridgeVia;

function isAdmissibleRecord(kind: "host" | "participant", value: unknown, trustedBridgeVia?: string): boolean {
  if (!isObject(value) || value.format !== 1 || typeof value.id !== "string" || value.id.length === 0) return false;
  // A relayed origin is admissible only when the receiving link's own hub attests the hop.
  if (trustedBridgeVia !== undefined && carriesRemoteHost(value) && !trustedRelayOf(value, trustedBridgeVia)) return false;
  if (kind === "host") {
    return typeof value.rootId === "string" && value.rootId.length > 0 && isIdentity(value.identity) &&
      finiteHostTimestamps(value as unknown as FabricHostRecord);
  }
  // The control protocol governs bridged roots; non-root rows are retained for reservation only.
  const controlProtocolValid = value.controlProtocol === "v1" || value.controlProtocol === "legacy" ||
    (value.kind !== "root" && value.controlProtocol === undefined);
  return typeof value.kind === "string" && PARTICIPANT_KINDS.has(value.kind) &&
    typeof value.rootId === "string" && value.rootId.length > 0 &&
    typeof value.ownerHostId === "string" && value.ownerHostId.length > 0 &&
    typeof value.ownerIdentityId === "string" && value.ownerIdentityId.length > 0 &&
    typeof value.name === "string" && typeof value.status === "string" &&
    typeof value.transport === "string" && PARTICIPANT_TRANSPORTS.has(value.transport) &&
    Array.isArray(value.capabilities) && value.capabilities.every(capability => typeof capability === "string") &&
    typeof value.startedAt === "number" && Number.isFinite(value.startedAt) &&
    typeof value.updatedAt === "number" && Number.isFinite(value.updatedAt) && controlProtocolValid;
}

const hostOf = (key: string, value: unknown, trustedBridgeVia?: string): FabricHostRecord | undefined => {
  const host = identifiableHostOf(key, value);
  return host && isAdmissibleRecord("host", host, trustedBridgeVia) ? host : undefined;
};

const presenceHostOf = (key: string, value: unknown, trustedBridgeVia?: string): FabricHostRecord | undefined =>
  hostOf(key, value, trustedBridgeVia) ?? (!carriesRemoteHost(value) ? identifiableHostOf(key, value) : undefined);

const participantOf = (key: string, value: unknown, trustedBridgeVia?: string): FabricParticipantRecord | undefined => {
  if (!isAdmissibleRecord("participant", value, trustedBridgeVia)) return undefined;
  const participant = value as FabricParticipantRecord;
  if (key !== keyFor(PARTICIPANT_PREFIX, participant.id)) return undefined;
  return participant;
};

const droppedPresenceOf = (type: BridgeDroppedPresence["type"], value: unknown, protect = true): BridgeDroppedPresence | undefined => {
  if (!isObject(value)) return undefined;
  const identityId = typeof value.identityId === "string" ? value.identityId
    : isObject(value.identity) && typeof value.identity.id === "string" ? value.identity.id : undefined;
  const dropped: BridgeDroppedPresence = {
    type,
    ...(protect ? {} : { protected: false }),
    ...(typeof value.id === "string" ? { id: value.id } : {}),
    ...(typeof value.rootId === "string" ? { rootId: value.rootId } : {}),
    ...(identityId !== undefined ? { identityId } : {}),
    ...(typeof value.ownerHostId === "string" ? { ownerHostId: value.ownerHostId } : {}),
    ...(typeof value.ownerIdentityId === "string" ? { ownerIdentityId: value.ownerIdentityId } : {}),
  };
  return Object.keys(dropped).length > 1 ? dropped : undefined;
};

const uniqueDroppedPresence = (values: readonly unknown[], preserveLocalProtection = false): BridgeDroppedPresence[] => {
  const unique = new Map<string, BridgeDroppedPresence>();
  for (const value of values) {
    if (!isObject(value) || (value.type !== "host" && value.type !== "participant")) continue;
    const dropped = droppedPresenceOf(value.type, value, !(preserveLocalProtection && value.protected === false));
    if (dropped) unique.set(JSON.stringify(dropped), dropped);
  }
  return [...unique.values()];
};

/** A content digest, independent of JSON property order (smarty-dev#5036). */
const contentDigest = (value: unknown): string => createHash("sha256").update(JSON.stringify(value, (_key, item) =>
  isObject(item) ? Object.fromEntries(Object.entries(item).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)) : item,
)).digest("hex");

// ---------------------------------------------------------------------------------------------
// Handoffs (smarty-dev#9074). A Main moved from the hub to a remote host keeps its session id, so
// its own dead native records on the hub reserve that id and the bridge refuses the moved Main
// forever. A handoff is a hub-side record, written only by the hub's local handoff command
// (bridge-handoff.ts) after it verified the native is dead and retired its records: "native
// identity <id> handed off to link <link>". While it is in force, the ids of that identity (its id,
// session id and name) are not reserved on that one link, so that link may mirror it; every other
// link stays refused, by the handoff itself. A native that answers to the id again, live or not,
// voids it (a live native always wins), and a bridge deletes a void handoff in its next mirror commit.

export const BRIDGE_HANDOFF_PREFIX = "topology/handoffs/";
/** Every state row a held event's ownership check reads (holds(): its records and #reservedNow()). */
const HELD_STATE_PREFIXES = [HOST_PREFIX, PARTICIPANT_PREFIX, BRIDGE_HANDOFF_PREFIX] as const;
/** #mirror: plans made before the transaction before the last one plans inside it. */
const MIRROR_PLAN_ATTEMPTS = 3;
/** A mirror plan made before the transaction no longer matches its snapshot: roll back, plan again. */
class MirrorPlanStale extends Error {}
/** #heldFence: pre-check attempts before the full check runs inside the fence. */
const HELD_FENCE_ATTEMPTS = 3;
const HELD_FENCE_STALE = Symbol("held fence stale");
const HOST_LEASE_DIR = "host-leases";
const PARTICIPANT_FILE_DIR = "participants";

export interface BridgeHandoff {
  format: 1;
  /** The handed-off root id (`session:<session id>`), its host and identity id alike. */
  id: string;
  sessionId: string;
  /** The root participant's name when it was handed off, if known. */
  name?: string;
  /** The one bridge link (its remote name) that may present this identity. */
  link: string;
  handedOffAt: number;
  operator: string;
  /** The hub host the handoff command ran on. */
  hub: string;
}

export const bridgeHandoffOf = (entry: Pick<MeshStateEntry, "key" | "value">): BridgeHandoff | undefined => {
  const value = entry.value;
  if (!isObject(value) || value.format !== 1 || typeof value.id !== "string" || !CANONICAL_ID.test(value.id)) return undefined;
  if (entry.key !== keyFor(BRIDGE_HANDOFF_PREFIX, value.id) || value.sessionId !== value.id.slice(ROOT_ID_PREFIX.length)) return undefined;
  if (typeof value.link !== "string" || !validBridgeName(value.link)) return undefined;
  if (typeof value.handedOffAt !== "number" || !Number.isFinite(value.handedOffAt)) return undefined;
  if (typeof value.operator !== "string" || !value.operator || typeof value.hub !== "string") return undefined;
  if (value.name !== undefined && (typeof value.name !== "string" || !value.name)) return undefined;
  return value as unknown as BridgeHandoff;
};

export interface BridgeHandoffScope {
  /** Ids handoffs reserve on this link: those of handoffs to other links, and of void ones. */
  reserved: string[];
  /** Whether a native record's reserved id is waived on this link by a handoff in force. */
  waived(value: unknown, id: string): boolean;
  /** Handoffs a native voided, for removal. */
  voided: MeshStateEntry[];
}

const NO_HANDOFFS: BridgeHandoffScope = { reserved: [], waived: () => false, voided: [] };

/** A lease only a native writes: a mirror's lease carries neither writer census data nor a Main session. */
const nativeLeaseLive = (lease: FabricHostLease, now: number): boolean =>
  (lease.writer !== undefined || lease.session !== undefined) &&
  Math.max(lease.expiresAt, lease.session?.expiresAt ?? -Infinity, lease.reloadUntil ?? -Infinity) >= now;

/** smarty-dev#10391: every lease bridgeHandoffScope judged at `then` is judged the same at `now`, either way. */
const sameLeaseLiveness = (leases: ReadonlyMap<string, FabricHostLease>, then: number, now: number,
  identifiers?: ReadonlySet<string>): boolean => [...leases.values()]
  .filter(lease => identifiers === undefined || identifiers.has(lease.id) || identifiers.has(lease.rootId) || identifiers.has(lease.identityId))
  .every(lease => nativeLeaseLive(lease, then) === nativeLeaseLive(lease, now));

/**
 * The handoffs as `peer`'s link sees them. A handoff is void when any native record (host or
 * participant, state or file, live or not) or a live native lease answers to its id: the handoff
 * command retired the dead native's records, so one there now is a native that came back.
 */
export const bridgeHandoffScope = (
  handoffEntries: readonly MeshStateEntry[],
  hostEntries: readonly MeshStateEntry[],
  participantEntries: readonly MeshStateEntry[],
  leases: () => ReadonlyMap<string, FabricHostLease>,
  now: number,
  peer: string,
): BridgeHandoffScope => {
  if (handoffEntries.length === 0) return NO_HANDOFFS;
  const answering = new Set<string>();
  const handoffIds = new Set(handoffEntries.flatMap(entry => {
    const handoff = bridgeHandoffOf(entry);
    return handoff ? [handoff.id] : [];
  }));
  for (const entry of [...hostEntries, ...participantEntries]) {
    if (!isObject(entry.value) || remoteHostOf(entry.value) !== undefined) continue;
    const value = entry.value;
    const ids = [value.id];
    if (entry.key.startsWith(HOST_PREFIX)) ids.push(value.rootId, isObject(value.identity) ? value.identity.id : undefined);
    for (const id of ids) if (typeof id === "string") answering.add(id);
  }
  for (const lease of leases().values()) {
    if (nativeLeaseLive(lease, now) && (handoffIds.has(lease.id) || handoffIds.has(lease.rootId) || handoffIds.has(lease.identityId))) {
      answering.add(lease.id).add(lease.rootId).add(lease.identityId);
    }
  }
  const reserved: string[] = [];
  const voided: MeshStateEntry[] = [];
  const held = new Map<string, Set<string>>();
  for (const entry of handoffEntries) {
    const handoff = bridgeHandoffOf(entry);
    // A malformed handoff grants nothing and reserves its id, like any malformed record.
    if (!handoff) {
      if (isObject(entry.value) && typeof entry.value.id === "string") reserved.push(entry.value.id);
      continue;
    }
    const ids = [handoff.id, handoff.sessionId, ...(handoff.name ? [handoff.name] : [])];
    const isVoid = answering.has(handoff.id);
    if (isVoid) voided.push(entry);
    if (isVoid || handoff.link !== peer) reserved.push(...ids);
    else held.set(handoff.id, new Set(ids));
  }
  if (held.size === 0) return { reserved, waived: () => false, voided };
  return {
    reserved,
    voided,
    // Only the identity's own native records (its leftover agents and actors), and only its own ids.
    waived: (value, id) => {
      if (!isObject(value) || remoteHostOf(value) !== undefined) return false;
      const owners = [value.id, value.rootId, value.ownerHostId, value.ownerIdentityId, isObject(value.identity) ? value.identity.id : undefined];
      return owners.some((owner) => typeof owner === "string" && held.get(owner)?.has(id) === true);
    },
  };
};

/** Host lease fields that change on every renewal; participant updatedAt is source activity. */
const settled = (value: Record<string, unknown>): string =>
  contentDigest({ ...value, updatedAt: undefined, expiresAt: undefined });

/**
 * A bridge side over a mesh store on this host. `peer` names the other side: it is the
 * `remoteHost` mark on mirrored records and the `bridge.from` of events it bridges in.
 */
interface BridgeLease { id: string; rootId: string; identityId: string; updatedAt: number; expiresAt: number; startedAt?: number; incarnationToken?: string; bridgeSupersededTokens?: string[]; bridgeSupersededTokenSeen?: Record<string, number>; historySeenFloor?: Record<string, number>; mirror?: FabricMirrorLease; mirrorLease?: 1 }
/**
 * A removed mirror's lease identity, as its lease file carries it (root, identity, start and token).
 * A row without root, identity and incarnation (an older release's) proves no owner: it removes nothing.
 */
interface BridgeUnlease { key: string; id: string; rootId?: string; identityId?: string; startedAt?: number; incarnationToken?: string }

const incarnationOf = (record: { startedAt?: unknown; incarnationToken?: unknown }): { startedAt?: number; incarnationToken?: string } => ({
  ...(typeof record.startedAt === "number" && Number.isFinite(record.startedAt) ? { startedAt: record.startedAt } : {}),
  ...(typeof record.incarnationToken === "string" && record.incarnationToken.length > 0 ? { incarnationToken: record.incarnationToken } : {}),
});

const BRIDGE_SUPERSEDED_TOKEN_LIMIT = 16;
const bridgeSupersededTokensOf = (value: unknown): string[] => {
  if (!Array.isArray(value)) return [];
  return [...new Set(value.filter((token): token is string => typeof token === "string" && token.length > 0))]
    .slice(-BRIDGE_SUPERSEDED_TOKEN_LIMIT);
};
type BridgeSupersededHistory = { tokens?: unknown; seen?: unknown };
const bridgeSupersededSeenOf = (value: unknown): Record<string, number> => {
  if (!isObject(value)) return {};
  const seen: Record<string, number> = Object.create(null);
  for (const [token, sequence] of Object.entries(value)) {
    if (token.length > 0 && Number.isSafeInteger(sequence) && (sequence as number) >= 0) seen[token] = sequence as number;
  }
  return seen;
};
const mergeBridgeSupersededHistory = (
  sources: readonly BridgeSupersededHistory[], additions: readonly string[] = [],
): { tokens: string[]; seen: Record<string, number>; firstSeen: Record<string, number> } => {
  const order: string[] = [];
  const sequences = new Map<string, number>();
  const origins = new Map<string, { source: number; index: number }>();
  for (const [sourceIndex, source] of sources.entries()) {
    for (const [token, sequence] of Object.entries(bridgeSupersededSeenOf(source.seen))) {
      sequences.set(token, Math.min(sequences.get(token) ?? Infinity, sequence));
    }
    for (const [index, token] of bridgeSupersededTokensOf(source.tokens).entries()) {
      if (!origins.has(token)) { order.push(token); origins.set(token, { source: sourceIndex, index }); }
    }
  }
  let next = -1;
  for (const sequence of sequences.values()) next = Math.max(next, sequence);
  next += 1;
  for (const token of additions) if (token && !order.includes(token)) {
    origins.set(token, { source: -1, index: order.length });
    order.push(token);
    sequences.set(token, next++);
  }
  // Missing ordinals are legacy evidence: preserve each chain's order and place secondary
  // chains before the primary chain when relative age is unknowable, so eviction favors authority.
  const sorted = order.map(token => ({ token, origin: origins.get(token)!, sequence: sequences.get(token) }))
    .sort((a, b) => a.sequence === undefined ? (b.sequence === undefined
      ? b.origin.source - a.origin.source || a.origin.index - b.origin.index
      : -1) : b.sequence === undefined ? 1
      : a.sequence - b.sequence || b.origin.source - a.origin.source || a.origin.index - b.origin.index);
  const retained = sorted.slice(-BRIDGE_SUPERSEDED_TOKEN_LIMIT);
  return { tokens: retained.map(item => item.token),
    seen: Object.fromEntries(retained.flatMap(item => item.sequence === undefined ? [] : [[item.token, item.sequence]])),
    firstSeen: Object.fromEntries(sequences) };
};

/** Refuse only a token proven superseded by this mesh's persisted local history. */
const olderBridgeIncarnation = (
  incoming: { startedAt?: unknown; incarnationToken?: unknown },
  supersededTokens: readonly string[],
): boolean => {
  const token = incarnationOf(incoming).incarnationToken;
  return token !== undefined && supersededTokens.includes(token);
};

/** Same-token metadata may change, but never lowers that owner's known finite age fence. */
const bridgeIncarnationWithFloor = (
  incoming: { startedAt?: unknown; incarnationToken?: unknown },
  authorities: readonly { startedAt?: number; incarnationToken?: string }[],
): { startedAt?: number; incarnationToken?: string } => {
  const next = incarnationOf(incoming);
  if (next.incarnationToken === undefined) return next;
  for (const held of authorities) {
    if (held.incarnationToken === next.incarnationToken && held.startedAt !== undefined && Number.isFinite(held.startedAt) &&
      (next.startedAt === undefined || held.startedAt > next.startedAt)) next.startedAt = held.startedAt;
  }
  return next;
};

export class StoreBridgeSide implements BridgeSide {
  // The mirror's host-lease writes and removals run after its state commit, from rows recorded in
  // that commit (smarty-dev#6477 R11): a crash in between leaves them for the next pass, which
  // replays them on its own snapshot (CommitOutboxPlan.replay) before it stages anything.
  readonly #outbox: CommitOutbox;
  #recovered = false;
  #leaseCustodyIds: ReadonlySet<string> | undefined;
  #expectedLeaseVersions: ReadonlyMap<string, number> | undefined;
  readonly #leaseSnapshotsByView = new WeakMap<object, ReadonlyMap<string, FabricHostLease>>();
  readonly #lastWrittenLeases = new Map<string, string>();
  #leaseEffectsTail: Promise<void> = Promise.resolve();
  // Active only during one mirror commit attempt, including recovery and post-commit retries.
  #skippedRoots: ReadonlySet<string> | undefined;
  #leaseProcess: Promise<BridgeLeaseProcess | undefined> | undefined;
  #mirrorDepth = 0;
  #mirrorIdle: Promise<void> = Promise.resolve();
  #resolveMirrorIdle: (() => void) | undefined;

  constructor(
    readonly store: MeshStore,
    readonly peer: string,
    readonly now: () => number = Date.now,
    /** CLI link/agent processes opt in; reference/manual adapters keep their existing lease shape. */
    readonly monitorLease = false,
    /** Run mirror lease file effects asynchronously after the state commit. */
    readonly deferLeaseInstalls = false,
  ) {
    if (!validBridgeName(peer)) throw new Error(`Invalid bridge peer name: ${peer}`);
    assertBridgeStateBackend(store.root, store.stateBackend);
    // A skipped root's effects are deferred, not successful no-ops: CommitOutbox catches the
    // throw and retains the row for a later legitimate renewal/removal (or its superseding effect).
    const retainSkipped = (rootId?: string): void => {
      if (rootId && this.#skippedRoots?.has(rootId)) throw new Error("Skipped root lease effect deferred");
    };
    this.#outbox = new CommitOutbox(store, `bridge/${peer}`, this.#identity(), {
      ...(!this.deferLeaseInstalls ? { lease: (lease: BridgeLease, view: MeshBatchView, replay: boolean) => {
        retainSkipped(lease.rootId);
        this.#requireLeaseCustody(lease.id);
        const key = keyFor(HOST_PREFIX, lease.id);
        const expected = this.#expectedLeaseVersions?.get(key);
        if (expected !== undefined && view.version(key) !== expected) return;
        this.#applyLeaseSync(lease, view, replay);
      } } : {}),
      // A removal (live or replayed) unlinks only the removed mirror's own lease: a replacement
      // owner that wrote its file lease before its state record keeps it (L2b owner review, P2).
      unlease: (gone: BridgeUnlease, view) => {
        retainSkipped(gone.rootId);
        this.#requireLeaseCustody(gone.id);
        const expected = this.#expectedLeaseVersions?.get(gone.key);
        if (expected !== undefined && view.version(gone.key) !== expected) return;
        if (view.get(gone.key) || typeof gone.rootId !== "string" || typeof gone.identityId !== "string" ||
          typeof gone.startedAt !== "number") return;
        // A lease without an incarnation is a mirror lease of an earlier release (or a writer from
        // before lease incarnations): root and identity decide it, as before.
        const removed = removeHostLeaseIf(this.store.root, gone.id, (lease) => lease.id === gone.id && lease.rootId === gone.rootId &&
          lease.identityId === gone.identityId && lease.incarnationToken === gone.incarnationToken &&
          (lease.startedAt === undefined || lease.startedAt === gone.startedAt));
        if (removed) this.#lastWrittenLeases.delete(gone.id);
      },
    }, this.deferLeaseInstalls ? {
      lease: async (lease: BridgeLease, view, replay) => {
        retainSkipped(lease.rootId);
        this.#requireLeaseCustody(lease.id);
        const key = keyFor(HOST_PREFIX, lease.id);
        const expected = this.#expectedLeaseVersions?.get(key);
        if (expected !== undefined && view.version(key) !== expected) return;
        await this.#applyLeaseAsync(lease, view, replay);
      },
    } : {});
  }

  // Each mirror put supplies its checked owner identity; this default is unused for deletes.
  #identity(): MeshIdentity {
    return { id: `bridge:${this.peer}`, name: this.peer, kind: "main" };
  }

  #requireLeaseCustody(id: string): void {
    if (!this.#leaseCustodyIds?.has(id)) throw new Error(`Bridge lease effect ran without host custody: ${id}`);
  }

  /** Wait for queued post-commit lease installs and their outbox retirement. */
  async flushLeaseEffects(): Promise<void> {
    await this.#leaseEffectsTail;
    await this.#outbox.retire();
  }

  async latestSequence(): Promise<number> {
    return this.store.latestSequence();
  }

  // A page fits one transport frame (security review F4): it ends before the event that would
  // pass the byte budget, and `through` stays at the last event it covers. One event that alone
  // passes the budget is skipped with evidence, never a stall of the cursor.
  async read(after: number, pageBytes = BRIDGE_PAGE_BYTES): Promise<BridgeRead> {
    return this.#page(this.store.read({ after, limit: this.store.maxReadEvents }), after, pageBytes);
  }

  async latestCursor(): Promise<BridgeHead> {
    const head = this.store.latestCursor();
    // An unreadable last complete line is not a sequence/offset anchor. Keep sequence
    // reconciliation until a publisher leaves a readable boundary, rather than skipping work.
    return head.last ? { through: head.last.sequence, offset: head.cursor } : { through: this.store.latestSequence() };
  }

  async tail(after: number, offset?: number, pageBytes = BRIDGE_PAGE_BYTES): Promise<BridgeRead> {
    if (offset !== undefined) {
      const page = this.store.tail(offset, this.store.maxReadEvents);
      // Compaction renames the live log before updating its separate generation file.
      // Even a matching generation/offset can therefore hide unread archived events.
      // Check before filtering topics or advancing through; a reserved sequence hole
      // is harmless unless the store actually holds an event inside the gap.
      const first = page.events.find(event => event.sequence > after);
      const gap = first !== undefined && first.sequence > after + 1 &&
        (this.store.nextEventAfter(after)?.sequence ?? Infinity) < first.sequence;
      if (!gap && meshCursorGeneration(page.nextOffset) === meshCursorGeneration(offset) && page.nextOffset >= offset) {
        return this.#page(page.events, after, pageBytes, { start: offset, cursors: page.cursors ?? [], end: page.nextOffset });
      }
      // A generation change, truncation or unread gap reconciles from the unchanged
      // sequence boundary via the archive; a raw tail reset alone would lose older work.
    }
    // Capture the committed boundary BEFORE the sequence read. Never adopt a later head
    // after an empty read: an append between those observations would be skipped forever.
    const head = await this.latestCursor();
    if (head.offset !== undefined && after >= head.through) return { events: [], through: after, offset: head.offset };
    const page = await this.read(after, pageBytes);
    return { ...page, ...(head.offset !== undefined && page.through >= head.through ? { offset: head.offset } : {}) };
  }

  #page(
    page: MeshEvent[], after: number, pageBytes: number,
    offsets?: { start: number; cursors: number[]; end: number },
  ): BridgeRead {
    const events: MeshEvent[] = [];
    const skipped: BridgeSkip[] = [];
    let bytes = 0;
    let through = after;
    let offset = offsets?.start;
    let complete = true;
    for (const [index, event] of page.entries()) {
      if (event.sequence > after && isBridgedTopic(event) && !isRelayReceipt(event) &&
          !(event.verification === "bridge" && event.topic === "fabric.control.command" && !isObject(event.data)) && (!hasBridgeField(event) ||
          (event.verification === "bridge" && (event.topic === "fabric.control.command" || event.topic === "fabric.control.ack") &&
            bridgeStampOf(event)?.from !== this.peer && !hasHubHop(event)))) {
        const size = Buffer.byteLength(JSON.stringify(event), "utf8");
        if (size > pageBytes) {
          skipped.push({ id: event.id, sequence: event.sequence, topic: event.topic, bytes: size });
        } else {
          if (bytes + size > pageBytes) { complete = false; break; }
          bytes += size;
          events.push(event);
        }
      }
      through = Math.max(through, event.sequence);
      if (offsets) offset = offsets.cursors[index] ?? offset;
    }
    if (offsets && complete) offset = offsets.end;
    return { events, through, ...(offset !== undefined ? { offset } : {}), ...(skipped.length ? { skipped } : {}) };
  }

  async presence(): Promise<BridgePresence> {
    return this.#presenceSnapshot();
  }

  #relayProjection(id: string): FabricHostRecord | undefined {
    const entry = this.store.get(keyFor(HOST_PREFIX, id), { fresh: true });
    const host = entry && hostOf(entry.key, entry.value);
    return host && host.remoteHost !== undefined && host.remoteHost !== this.peer && host.bridgeVia === undefined &&
      CANONICAL_ID.test(id) && host.rootId === id && host.identity.id === id &&
      !participantFilePresent(this.store.root, keyFor(PARTICIPANT_PREFIX, id)) &&
      entry!.updatedBy.id === host.identity.id ? host : undefined;
  }

  relayOrigin(id: string): string | undefined {
    const host = this.#relayProjection(id);
    return host && hostLeaseExpiry(readHostLeases(this.store.root), host) > this.now() ? host.remoteHost : undefined;
  }

  relayStaleOrigin(id: string): string | undefined {
    const host = this.#relayProjection(id);
    return host && hostLeaseExpiry(readHostLeases(this.store.root), host) <= this.now() ? host.remoteHost : undefined;
  }

  async relayPresence(): Promise<Pick<BridgePresence, "hosts" | "participants">> {
    const hosts: BridgeHost[] = [];
    const leases = readHostLeases(this.store.root);
    for (const entry of this.store.listAll(HOST_PREFIX, { fresh: true })) {
      const host = hostOf(entry.key, entry.value);
      if (!host || host.remoteHost === undefined || host.remoteHost === this.peer || host.bridgeVia !== undefined ||
          !CANONICAL_ID.test(host.id) || host.rootId !== host.id || host.identity.id !== host.id ||
          participantFilePresent(this.store.root, keyFor(PARTICIPANT_PREFIX, host.id)) ||
          entry.updatedBy.id !== host.identity.id) continue;
      const liveness = hostLiveness(leases, host);
      const observedAt = this.now();
      if (liveness.expiresAt > observedAt) hosts.push({ record: { ...host, ...liveness }, expiresAt: liveness.expiresAt, observedAt });
    }
    const owners = new Map(hosts.map(host => [host.record.id, host.record]));
    const participants = this.store.listAll(PARTICIPANT_PREFIX, { fresh: true }).flatMap(entry => {
      const participant = participantOf(entry.key, entry.value);
      const owner = participant && owners.get(participant.ownerHostId);
      return participant && owner && participant.kind === "root" && participant.remoteHost === owner.remoteHost &&
        participant.bridgeVia === undefined && participant.id === owner.id && participant.rootId === owner.rootId &&
        participant.ownerIdentityId === owner.identity.id && entry.updatedBy.id === owner.identity.id ? [participant] : [];
    });
    return { hosts, participants };
  }

  #presenceSnapshot(): BridgePresence {
    return this.#presence(this.store.listAll(HOST_PREFIX, { fresh: true }), this.#participantEntries(),
      this.store.listAll(BRIDGE_HANDOFF_PREFIX, { fresh: true }));
  }

  subscribeChanges(changed: (kind: BridgeChange) => void, failed: (error: Error) => void): () => void {
    let active = true;
    let queued = false;
    let retryTimer: ReturnType<typeof setTimeout> | undefined;
    let burst: ReturnType<typeof setImmediate> | undefined;
    let scanNext = false;
    let eventsNext = false;
    let retryMs = LOCK_RETRY_MIN_MS;
    let baseline: ReturnType<typeof snapshot> | undefined;
    // File snapshots are private, copy-on-write parses. Observe their read-only entries once,
    // instead of cloning the whole topology for each projection on every catch-up notification.
    // Cache content by object, never by version: legacy writers can reuse revision labels.
    const fileState = this.store.stateBackend === "file";
    const digests = new WeakMap<object, string>();
    const fingerprint = (value: object): string => {
      let digest = digests.get(value);
      if (digest === undefined) { digest = contentDigest(value); digests.set(value, digest); }
      return digest;
    };
    const snapshot = () => {
      const leases = readHostLeases(this.store.root);
      const hostEntries = fileState ? this.store.listAllShared(HOST_PREFIX, { fresh: true }) : undefined;
      const participantEntries = fileState ? this.store.listAllShared(PARTICIPANT_PREFIX, { fresh: true }) : undefined;
      // Include held projection existence/content, so an external prune/lease removal repairs
      // without waiting for traffic. Exclude our renewal fields: our own mirror writes then
      // cause at most one settling pass, never a self-sustaining lease-write feedback loop.
      const mirrored = [...(hostEntries ?? this.store.listAll(HOST_PREFIX, { fresh: true })),
        ...(participantEntries ?? this.store.listAll(PARTICIPANT_PREFIX, { fresh: true }))]
        .filter(entry => mirrorLinkOf(entry.value) === this.peer)
        .map(entry => {
          const host = hostOf(entry.key, entry.value);
          const lease = host && leases.get(host.id);
          return { key: entry.key, value: isObject(entry.value) && entry.key.startsWith(HOST_PREFIX) ? settled(entry.value)
            : fileState && isObject(entry.value) ? fingerprint(entry.value) : entry.value,
            updatedBy: entry.updatedBy,
            ...(host ? { lease: lease ? { id: lease.id, rootId: lease.rootId, identityId: lease.identityId,
              startedAt: lease.startedAt, incarnationToken: lease.incarnationToken } : null } : {}) };
        }).sort((a, b) => a.key.localeCompare(b.key));
      const native = hostEntries && participantEntries ? this.#presence(hostEntries,
        [...participantEntries, ...readParticipantFiles(this.store.root, { maxAgeMs: 0 })],
        this.store.listAllShared(BRIDGE_HANDOFF_PREFIX, { fresh: true })) : this.#presenceSnapshot();
      // Other links' mirror additions/removals must wake the hub's exporting links.
      const relays = [...(hostEntries ?? this.store.listAll(HOST_PREFIX, { fresh: true })),
        ...(participantEntries ?? this.store.listAll(PARTICIPANT_PREFIX, { fresh: true }))]
        .filter(entry => remoteHostOf(entry.value) !== undefined && mirrorLinkOf(entry.value) !== this.peer)
        .map(entry => ({ key: entry.key, value: entry.key.startsWith(HOST_PREFIX) && isObject(entry.value)
          ? settled(entry.value) : fileState && isObject(entry.value) ? fingerprint(entry.value) : entry.value, updatedBy: entry.updatedBy }));
      const projection = fileState ? { ...native, participants: native.participants.map(fingerprint) } : native;
      const topology = { ...projection, hosts: native.hosts.map(host => settled(host.record as unknown as Record<string, unknown>)) };
      const content = { ...projection, hosts: native.hosts.map(({ observedAt: _observedAt, ...host }) => host) };
      return { native, digest: contentDigest({ native: content, mirrored, relays }), topology: contentDigest({ topology, mirrored, relays }) };
    };
    // A WAL write can be observed before the writer's COMMIT/wal-index publication. A
    // read then sees old state and may receive no later fs event. Fence the observation
    // against the pending writer, retrying actual contention only, never idle polling.
    // smarty-dev#10391: the snapshot reads every host, participant and handoff row and the participant and
    // lease files. Taken inside the fence, it held state.db's write lock on EVERY change notification of the
    // mesh, for each bridge: on a busy hub each state write woke every bridge into a full scan under the lock,
    // so the bridges' share grew with the load (the idle spin). It is taken first; the fence then only proves
    // that no such row changed since its revision (the pending writer has committed by then) and that both
    // file directories kept their stamps. A failed proof takes it again; the last try runs inside the fence.
    const files = (): string => `${meshDirectoryStamp(this.store.root, PARTICIPANT_FILE_DIR)}|${meshDirectoryStamp(this.store.root, HOST_LEASE_DIR)}`;
    const capture = (): ReturnType<typeof snapshot> => {
      if (this.store.stateBackend !== "sqlite") return snapshot();
      for (let attempt = 0; attempt < HELD_FENCE_ATTEMPTS - 1; attempt += 1) {
        const revision = this.store.stateRevision();
        const stamps = files();
        const value = snapshot();
        if (revision !== undefined && this.store.withStateWriteFence(() =>
          this.store.stateUnchangedSince(revision, HELD_STATE_PREFIXES) && files() === stamps)) return value;
      }
      return this.store.withStateWriteFence(snapshot);
    };
    const flush = (): void => {
      queued = false;
      if (!active) return;
      try {
        const next = capture();
        retryMs = LOCK_RETRY_MIN_MS;
        if (!baseline || next.digest !== baseline.digest) {
          const previousExpiry = new Map(baseline?.native.hosts.map(host => [host.record.id, host.expiresAt]) ?? []);
          const shortened = next.native.hosts.some(host => host.expiresAt < (previousExpiry.get(host.record.id) ?? -Infinity));
          const kind = !baseline || next.topology !== baseline.topology || shortened ? "presence" : "lease";
          baseline = next;
          changed(kind);
        }
      } catch (error) {
        if (isMeshRetryableBusy(error)) {
          queued = true;
          retryTimer = setTimeout(() => { retryTimer = undefined; flush(); }, retryMs);
          retryMs = Math.min(retryMs * 2, LOCK_RETRY_MAX_MS);
        } else failed(error instanceof Error ? error : new Error(String(error)));
      }
    };
    // One check-phase tick per notification burst: the burst's scan (if any) first, then its event wake.
    // A lease renewal followed by routed work arrives as one burst; classifying the lease before the
    // directions wake lets their authority check see the pending lease and refresh it now, instead of
    // forwarding under the old mirror and leaving the renewal to the half-lease flush.
    const tick = (): void => {
      burst = undefined;
      if (!active) return;
      if (scanNext) { scanNext = false; flush(); }
      if (eventsNext && active) { eventsNext = false; changed("events"); }
    };
    const close = watchBridgeStore(this.store.root, (kind) => {
      // Coalesce one notification burst into one scan. Each fs.watch callback is its own native callback
      // (microtasks drain between them), so a microtask flush scanned once per notification. setImmediate
      // runs after the whole poll phase has delivered its notifications; any arriving while one is queued or
      // a busy retry waits join it, and a later burst queues at most one trailing scan. Events never scan
      // and never wait for a busy retry: they wait at most for this tick's own scan.
      if (kind === "events") eventsNext = true;
      else if (queued) return;
      else { queued = true; scanNext = true; }
      burst ??= setImmediate(tick);
    }, failed);
    try { baseline = capture(); }
    catch (error) {
      if (!isMeshRetryableBusy(error)) { close(); throw error; }
      // Keep installed watches and let the other side subscribe. A zero-wait SQLite
      // baseline fence can starve under sustained writes; throwing here blocks every
      // post-link mirror and even watchdog startup. Retry actual contention as in flush.
      queued = true;
      retryTimer = setTimeout(() => { retryTimer = undefined; flush(); }, retryMs);
      retryMs = Math.min(retryMs * 2, LOCK_RETRY_MAX_MS);
    }
    return () => {
      active = false;
      if (retryTimer !== undefined) clearTimeout(retryTimer);
      if (burst !== undefined) clearImmediate(burst);
      close();
    };
  }

  #presence(hostEntries: readonly MeshStateEntry[], participantEntries: readonly MeshStateEntry[], handoffEntries: readonly MeshStateEntry[]): BridgePresence {
    return this.#scan(hostEntries, participantEntries, handoffEntries).presence;
  }

  #scan(
    hostEntries: readonly MeshStateEntry[], participantEntries: readonly MeshStateEntry[], handoffEntries: readonly MeshStateEntry[],
    leaseSnapshot?: ReadonlyMap<string, FabricHostLease>,
  ): { presence: BridgePresence; voided: MeshStateEntry[]; leases: ReadonlyMap<string, FabricHostLease>; now: number } {
    const now = this.now();
    const leases = leaseSnapshot ?? readHostLeases(this.store.root);
    const handoffs = bridgeHandoffScope(handoffEntries, hostEntries, participantEntries, () => leases, now, this.peer);
    const reserved = new Set<string>(handoffs.reserved);
    // Only a handoff in force to this link waives a (dead) native's own ids (smarty-dev#9074).
    const reserve = (value: unknown, id: string): void => { if (!handoffs.waived(value, id)) reserved.add(id); };
    const hosts: BridgeHost[] = [];
    const nonRootHosts: BridgeHost[] = [];
    let rawDroppedCount = 0;
    // Every id not mirrored by this link is reserved: natives, live or not, and other links'
    // mirrors (security review F1/F2). Only natives are this side's presence.
    for (const entry of hostEntries) {
      const mark = remoteHostOf(entry.value);
      if (mirrorLinkOf(entry.value) === this.peer) continue;
      const host = presenceHostOf(entry.key, entry.value);
      if (!host) {
        if (mark === undefined) rawDroppedCount += 1;
        if (isObject(entry.value) && typeof entry.value.id === "string") reserved.add(entry.value.id);
        continue;
      }
      for (const id of [host.id, host.identity.id, host.rootId]) reserve(entry.value, id);
      if (mark !== undefined) continue;
      if (entry.updatedBy.id !== host.identity.id) { rawDroppedCount += 1; continue; }
      if (isNativeNonRootHost(host)) {
        const liveness = hostLiveness(leases, host);
        if (liveness.expiresAt > now) nonRootHosts.push({ record: { ...host, ...liveness }, expiresAt: liveness.expiresAt, observedAt: now });
        continue;
      }
      if (!isAdmissibleRecord("host", host)) {
        rawDroppedCount += 1;
        // Do not let effective file liveness normalize a bad native/bootstrap record into
        // renewable authority, or silently omit it and erase the peer's retained fence.
        hosts.push({ record: host, expiresAt: host.expiresAt, observedAt: now });
        continue;
      }
      const liveness = hostLiveness(leases, host);
      const expiresAt = liveness.expiresAt;
      // Carry effective renewal time as well as expiry, with the same incarnation fence.
      const record = { ...host, ...liveness };
      if (expiresAt > now) hosts.push({ record, expiresAt, observedAt: now });
    }
    const live = new Map(hosts.map((host) => [host.record.id, host.record]));
    const participants = new Map<string, { record: FabricParticipantRecord; updatedAt: number }>();
    const participantCopies = new Map<string, { record: FabricParticipantRecord; updatedAt: number }>();
    for (const entry of participantEntries) {
      const mark = remoteHostOf(entry.value);
      if (mirrorLinkOf(entry.value) === this.peer) continue;
      const participant = participantOf(entry.key, entry.value);
      if (!participant) {
        if (mark === undefined) rawDroppedCount += 1;
        if (isObject(entry.value) && typeof entry.value.id === "string") reserved.add(entry.value.id);
        continue;
      }
      const copy = participantCopies.get(entry.key);
      if (copy && settled(copy.record as unknown as Record<string, unknown>) !==
        settled(participant as unknown as Record<string, unknown>)) rawDroppedCount += 1;
      if (!copy || copy.updatedAt <= entry.updatedAt) participantCopies.set(entry.key, { record: participant, updatedAt: entry.updatedAt });
      for (const id of [participant.id, participant.ownerIdentityId, participant.rootId]) reserve(entry.value, id);
      if (typeof participant.name === "string" && participant.name) reserve(entry.value, participant.name);
      // Labels are minted per mesh and collide by design; they are never addresses across.
      if (typeof participant.sessionId === "string" && participant.sessionId) reserve(entry.value, participant.sessionId);
      const owner = live.get(participant.ownerHostId);
      if (mark === undefined && participant.kind === "root" && owner && entry.updatedBy.id === participant.ownerIdentityId &&
        owner.identity.id === participant.ownerIdentityId && owner.rootId === participant.rootId) {
        const previous = participants.get(participant.id);
        // The source directory's rule is later write wins, with the participant file winning ties.
        // #participantEntries lists shared state first and files second, so replacing on >= preserves it.
        if (!previous || previous.updatedAt <= entry.updatedAt) {
          participants.set(participant.id, { record: participant, updatedAt: entry.updatedAt });
        }
      } else if (mark === undefined && participant.kind === "root" && owner !== undefined) rawDroppedCount += 1;
    }
    const admitted = admitPresence({ hosts, participants: [...participants.values()].map(({ record }) => record),
      ...(rawDroppedCount > 0 ? { partial: true, droppedCount: rawDroppedCount } : {}) }, new Set());
    return {
      // Root admission exports canonical root hosts; valid resident/runtime rows travel separately
      // for remote directories and reapers, never as address authority. Reservations above still
      // cover ALL native and other-link identities, live or not.
      presence: { ...admitted, hosts: [...admitted.hosts, ...nonRootHosts], reserved: [...reserved] },
      voided: handoffs.voided,
      // smarty-dev#10391: what the scan judged by time, for a plan made before its transaction.
      leases, now,
    };
  }

  // Participant entries of both kinds: the shared state's (natives and mirrors) and the natives'
  // own files (smarty-dev#2004; under the participants-files policy natives are only there).
  #participantEntries(): MeshStateEntry[] {
    return [...this.store.listAll(PARTICIPANT_PREFIX, { fresh: true }), ...readParticipantFiles(this.store.root, { maxAgeMs: 0 })];
  }

  // Do not retry a busy SQLite ownership fence inside bridge publication: failure guarantees no append.
  // Let the per-direction loop back off with .lock released instead of reacquiring ahead of queued writers.
  async publish(event: BridgePublish, held: string[] = [], fromHub = false): Promise<{ sequence: number }> {
    await this.store.prepareLock();
    const published = await this.store.publish(this.#publication(event, held, fromHub));
    return { sequence: published.sequence };
  }

  async publishWithReceipt(event: BridgePublish, held: string[] = [], fromHub = false): Promise<BridgePublishReceipt> {
    const { sequence } = await this.publish(event, held, fromHub);
    const appended = this.store.read({ after: sequence - 1, limit: 1 })[0];
    return { sequence, ...(appended?.sequence === sequence ? { createdAt: appended.createdAt } : {}) };
  }

  async recordRelay(event: BridgePublish, held: string[] = [event.from.id]): Promise<{ sequence: number }> {
    const checked = checkBridgePublish(event);
    const stamp = checked.data.bridge;
    if (checked.topic !== "fabric.control.command" || stamp.from !== this.peer || hasHubHop(checked)) {
      throw new BridgeOwnershipError("Invalid relay receipt origin");
    }
    // The dedupe receipt stores this envelope too: NEVER pass the command body to store.publish.
    const key = `${RELAY_RECEIPT_PREFIX}${encodeURIComponent(this.peer)}:${encodeURIComponent(stamp.id)}`;
    await this.store.prepareLock();
    const published = await this.store.publish({
      id: key, dedupeKey: key, topic: checked.topic, kind: checked.kind,
      from: { ...checked.from, verified: "bridge" }, to: checked.to, durable: true,
      ...this.#heldFence([...new Set([checked.from.id, ...held])], checked.from),
      // createdAt is stamped by the host store at commit; no data/text/message/principal.
    });
    return { sequence: published.sequence };
  }

  async publishBatch(events: Array<{ event: BridgePublish; held?: string[] }>, fromHub = false): Promise<Array<{ sequence: number }>> {
    await this.store.prepareLock();
    const published = await this.store.publishBatch(events.map(({ event, held }) => this.#publication(event, held ?? [], fromHub)));
    return published.map(({ sequence }) => ({ sequence }));
  }

  async publishBatchWithReceipt(events: Array<{ event: BridgePublish; held?: string[] }>, fromHub = false): Promise<BridgePublishReceipt[]> {
    const published = await this.publishBatch(events, fromHub);
    return this.publishReceipts(published.map(({ sequence }) => sequence));
  }

  publishReceipts(sequences: readonly number[]): BridgePublishReceipt[] {
    if (!sequences.length || sequences.length > 256 || sequences.some(sequence => !Number.isSafeInteger(sequence) || sequence <= 0)) {
      throw new Error("Invalid bridge publish receipt sequences");
    }
    const first = Math.min(...sequences);
    const last = Math.max(...sequences);
    if (last - first >= 256) throw new Error("Bridge publish receipt range is too large");
    const bySequence = new Map(this.store.read({ after: first - 1, limit: last - first + 1 }).map(event => [event.sequence, event.createdAt]));
    return sequences.map(sequence => ({ sequence, ...(bySequence.has(sequence) ? { createdAt: bySequence.get(sequence)! } : {}) }));
  }

  #publication(event: BridgePublish, held: string[], fromHub: boolean): Parameters<MeshStore["publish"]>[0] {
    if (this.#staleAdmission) throw this.#staleAdmission;
    const checked = checkBridgePublish(event);
    const stamp = checked.data.bridge;
    const data = { ...checked.data, bridge: fromHub && stamp.via === this.peer
      ? stamp : { from: this.peer, id: stamp.id } };
    return {
      ...checked,
      // The cursor advances only after the destination confirms its append durably.
      durable: true,
      // Only controls need durable retry receipts. Ordinary events retain the batched
      // live-log barrier and cursor reconciliation, without per-event receipt writes.
      // Commands use the logical command id; ACKs use the original bridge stamp.
      ...(checked.topic === "fabric.control.command" || checked.topic === "fabric.control.ack"
        ? { dedupeKey: `bridge-publish:${contentDigest(checked.topic === "fabric.control.command" &&
          typeof checked.data.commandId === "string" && checked.data.commandId.length > 0
          ? [data.bridge.from, checked.from.id, checked.data.commandId]
          : [data.bridge.from, data.bridge.id])}` } : {}),
      // Host-only: bind the destination receipt to canonical semantic input, never a
      // wire-supplied fingerprint. Hub recordRelay envelopes deliberately omit it.
      ...(checked.topic === "fabric.control.command"
        ? { dedupeFingerprint: controlBodyFingerprint(checked.data, checked.principal) } : {}),
      from: { ...checked.from, verified: "bridge" },
      // The data is fixed, so the store encodes it before the lock (smarty-dev#6729); the
      // ownership check is its admission step. Evaluated under the mesh lock that commits the
      // event, so the ownership it checks is the ownership at commit: a native takeover before
      // it refuses the event (security review round 3, F2). Every state writer takes the same
      // lock on the `file` backend. SQLite state writers do not take `.lock`, so a held event
      // also runs in the state write fence (plan R20, smarty-dev#6477 L2b owner review P1): with
      // `.lock` held, one `BEGIN IMMEDIATE`, then this check on that transaction's snapshot and
      // the synchronous append, then ROLLBACK. A takeover's commit waits for the append, or
      // commits first and this check refuses. Lock order is `.lock`, then the SQLite write lock,
      // never the reverse (a SQLite transaction is one synchronous segment and `.lock` is only
      // acquired asynchronously). Under the participants-files policy a native's first file is
      // written without it; its host record, which reserves the root id, still goes through the
      // lock, and a mirror never outranks a native file (#142 S2).
      ...(held.length > 0 ? this.#heldFence(held, checked.from) : {}),
      data,
    };
  }

  /**
   * Whether this link holds `id` now. Run under the mesh lock that commits a bridged event, and in
   * its state write fence (R20), so it decides on the ownership at commit (security review rounds
   * 3 and 4, F2). Any record for the
   * id that is not this link's live mirror (a native, another link's, or a reserved id anywhere
   * on this side) is a denial, never a gap to fill from another mirror: `id` must be this
   * link's host, with a live lease, and its root, if present, must be this link's too. With
   * `identity` (an inbound sender's `from`), that must also be exactly the host record's identity:
   * id, name, kind and session id (pi-fabric#876 security pass, (b)).
   */
  holds(id: string, identity?: MeshIdentity): boolean {
    if (this.#staleAdmission) return false;
    // Inside #heldFence, after its proof: this exact (id, identity) was checked at a revision the fence
    // proved current. Set and cleared in one synchronous segment, so no other caller can see it.
    if (this.#proven.has(id) && this.#proven.get(id) === identity) return true;
    return this.#heldUntil(id, identity) > this.now();
  }

  /** #heldFence's proven pre-checks, by id, live only inside its fence (see holds()). */
  readonly #proven = new Map<string, MeshIdentity | undefined>();

  /**
   * smarty-dev#10391: the ownership check of a held event, split around the SQLite write fence. The full check
   * (every host, participant and handoff row, the participant and lease files) reads O(fleet) data: inside the
   * fence it held state.db's write lock for each bridged event, so a bridge behind on a large hub kept the lock
   * busy for its whole catch-up. It now runs just before the fence, after recording the state revision and the
   * stamps of the files it reads. In the fence, with the time read there, a cheap proof stands in for it: no host,
   * participant or handoff row changed since (the change feed), no participant file was added, replaced or
   * removed (the directory stamp), the lease files it used are unchanged (each held id's own file; the whole
   * directory when a handoff row exists, since handoffs read every lease), and every held lease is still live.
   * A failed proof releases the fence and checks again, twice; the third attempt runs the full check in the
   * fence, as before. admit() still calls holds() for each id inside the fence; after the proof, holds()
   * answers from the proven pre-check. A link that does not hold an id at the pre-check refuses it without
   * taking the fence (refusing is fail-closed; the caller's one refresh-and-retry still applies). The file
   * backend (no state revision) keeps the full check in its `.lock` passthrough fence, unchanged.
   */
  #heldFence(held: readonly string[], from: MeshIdentity): Pick<MeshPublishInput, "fence" | "admit"> {
    const identityOf = (id: string): MeshIdentity | undefined => id === from.id ? from : undefined;
    return {
      fence: <R>(commit: () => R): R => {
        for (let attempt = 0; ; attempt += 1) {
          const observed = attempt < HELD_FENCE_ATTEMPTS - 1 ? this.#heldObservation(held, from) : undefined;
          const outcome = this.store.withStateWriteFence(() => {
            if (observed !== undefined) {
              if (!(observed.until > this.now() && this.store.stateUnchangedSince(observed.revision, HELD_STATE_PREFIXES) &&
                this.#heldFiles(held, observed.leasesAll) === observed.files)) return HELD_FENCE_STALE;
              for (const id of held) this.#proven.set(id, identityOf(id));
            }
            try { return { value: commit() }; }
            finally { this.#proven.clear(); }
          });
          if (outcome !== HELD_FENCE_STALE) return outcome.value;
        }
      },
      admit: () => {
        for (const id of held) {
          // An inbound sender's name, kind and session id must be its mirrored record's, at commit
          // (pi-fabric#876 security pass, (b)): a body identity never gets the bridge's mark.
          if (!this.holds(id, identityOf(id))) {
            throw new BridgeOwnershipError(`${id} is no longer bound to bridge link ${this.peer}`);
          }
        }
      },
    };
  }

  // The pre-check of #heldFence: undefined when it cannot run outside the fence (no SQLite state revision).
  // Throws BridgeOwnershipError when this link does not hold an id now.
  #heldObservation(held: readonly string[], from: MeshIdentity):
    { revision: string; until: number; files: string; leasesAll: boolean } | undefined {
    const revision = this.store.stateRevision();
    if (revision === undefined) return undefined;
    // Handoff rows are proven by the change feed; with none, no lease but the held ids' own is read.
    const leasesAll = this.store.list(BRIDGE_HANDOFF_PREFIX, 1, { fresh: true }).length > 0;
    const files = this.#heldFiles(held, leasesAll);
    let until = Infinity;
    for (const id of held) {
      const expiry = this.#heldUntil(id, id === from.id ? from : undefined);
      if (expiry <= this.now()) throw new BridgeOwnershipError(`${id} is no longer bound to bridge link ${this.peer}`);
      until = Math.min(until, expiry);
    }
    return { revision, until, files, leasesAll };
  }

  // Stamps of the files the ownership check reads: one stat per held lease file (or the lease directory) and
  // the participant directory. Atomic writes rename into the directory, so any change moves its stamp.
  #heldFiles(held: readonly string[], leasesAll: boolean): string {
    const stat = (file: string): string => {
      try {
        const value = fs.statSync(file, { bigint: true });
        return `${value.ino}:${value.mtimeNs}:${value.ctimeNs}:${value.size}`;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") return "-";
        throw error;
      }
    };
    const leases = leasesAll ? [meshDirectoryStamp(this.store.root, HOST_LEASE_DIR)]
      : held.map((id) => stat(hostLeasePath(this.store.root, id)));
    return [meshDirectoryStamp(this.store.root, PARTICIPANT_FILE_DIR), ...leases].join("|");
  }

  // The lease expiry of `id` when this link holds it now (see holds()), else 0.
  #heldUntil(id: string, identity?: MeshIdentity): number {
    const now = this.now();
    const hostEntry = this.store.get(keyFor(HOST_PREFIX, id), { fresh: true });
    const participantEntry = this.store.get(keyFor(PARTICIPANT_PREFIX, id), { fresh: true });
    if (!hostEntry || mirrorLinkOf(hostEntry.value) !== this.peer) return 0;
    if (participantEntry && mirrorLinkOf(participantEntry.value) !== this.peer) return 0;
    // A native's own file, readable or not (fail closed, security pass S3 on #142).
    if (participantFilePresent(this.store.root, keyFor(PARTICIPANT_PREFIX, id))) return 0;
    const host = hostOf(hostEntry.key, hostEntry.value);
    if (!host || host.id !== id || host.identity.id !== id || host.rootId !== id || hostEntry.updatedBy.id !== id) return 0;
    if (identity && !sameIdentity(host.identity, identity)) return 0;
    // hostLeaseExpiry consults only leases.get(host.id) (host.id === id above), and a lease file is
    // valid only under its own id's name: read that one file, not the whole lease directory (a
    // readdir plus a stat per fleet lease, per held event, on Windows where listings are never cached).
    const lease = readHostLease(this.store.root, id);
    const until = hostLeaseExpiry(new Map<string, FabricHostLease>(lease ? [[id, lease]] : []), host);
    if (until <= now) return 0;
    if (participantEntry) {
      const participant = participantOf(participantEntry.key, participantEntry.value);
      if (!participant || participant.ownerHostId !== id || participant.ownerIdentityId !== id) return 0;
    }
    return this.#reservedNow().has(id) ? 0 : until;
  }

  mirrored(id: string): boolean {
    return mirrorLinkOf(this.store.get(keyFor(HOST_PREFIX, id), { fresh: true })?.value) === this.peer;
  }

  // Every id this side holds that is not this link's mirror, read now, under the same handoffs.
  #reservedNow(): Set<string> {
    const hostEntries = this.store.listAll(HOST_PREFIX, { fresh: true });
    const participantEntries = this.#participantEntries();
    const handoffs = bridgeHandoffScope(this.store.listAll(BRIDGE_HANDOFF_PREFIX, { fresh: true }), hostEntries, participantEntries,
      () => readHostLeases(this.store.root), this.now(), this.peer);
    const reserved = new Set<string>(handoffs.reserved);
    for (const entries of [hostEntries, participantEntries]) {
      for (const entry of entries) {
        if (mirrorLinkOf(entry.value) === this.peer || !isObject(entry.value)) continue;
        const value = entry.value;
        const ids = ["id", "rootId", "ownerHostId", "ownerIdentityId", "sessionId", "name"].map((field) => value[field]);
        if (isIdentity(value.identity)) ids.push(value.identity.id);
        for (const id of ids) if (typeof id === "string" && id && !handoffs.waived(value, id)) reserved.add(id);
      }
    }
    return reserved;
  }


  #fenced = false;
  #staleAdmission: BridgeStaleIncarnationError | undefined;
  #inflight: Promise<void> = Promise.resolve();
  #hasCommittedPass = false;

  /**
   * Mirror the peer's presence. Calls run one at a time; after withdraw() no call writes again,
   * and each write checks the fence, so a mirror in flight cannot restore a record or lease after
   * the withdrawal (security review round 2, F3).
   */
  mirror(presence: BridgeMirrorPresence, observedAt?: number, fromHub = false): Promise<void> {
    if (this.#staleAdmission) return Promise.reject(this.#staleAdmission);
    if (this.#fenced) return Promise.resolve();
    // Only the forced-command agent sets fromHub for its trusted hub's mirror RPC.
    const run = this.#inflight.then(() => this.#mirror(presence, false, observedAt, fromHub)).catch((error: unknown) => {
      if (error instanceof BridgeStaleIncarnationError) {
        // This instance never acquired the newer incarnation. Fence queued writes and keep
        // cleanup from withdrawing its predecessor's persisted authority after the refusal.
        this.#staleAdmission = error;
        this.#fenced = true;
      }
      throw error;
    });
    this.#inflight = run.catch(() => undefined);
    return run;
  }

  async fence(): Promise<void> {
    this.#fenced = true;
    await this.#inflight;
  }

  async withdraw(): Promise<void> {
    await this.fence();
    if (this.#staleAdmission) return;
    await this.#mirror({ hosts: [], participants: [] }, true);
  }

  async owned(): Promise<Pick<BridgePresence, "hosts" | "participants">> {
    const now = this.now();
    const leases = readHostLeases(this.store.root);
    const hosts: BridgeHost[] = [];
    for (const entry of this.store.listAll(HOST_PREFIX, { fresh: true })) {
      if (mirrorLinkOf(entry.value) !== this.peer) continue;
      const host = hostOf(entry.key, entry.value);
      if (!host || entry.updatedBy.id !== host.identity.id) continue;
      const expiresAt = hostLeaseExpiry(leases, host);
      if (expiresAt > now) hosts.push({ record: { ...host, ...hostLiveness(leases, host) }, expiresAt });
    }
    const live = new Map(hosts.map((host) => [host.record.id, host.record]));
    const participants: FabricParticipantRecord[] = [];
    for (const entry of this.store.listAll(PARTICIPANT_PREFIX, { fresh: true })) {
      if (mirrorLinkOf(entry.value) !== this.peer) continue;
      const participant = participantOf(entry.key, entry.value);
      const owner = participant && live.get(participant.ownerHostId);
      if (
        participant && owner && participant.kind === "root" && entry.updatedBy.id === participant.ownerIdentityId &&
        owner.identity.id === participant.ownerIdentityId && owner.rootId === participant.rootId
      ) participants.push(participant);
    }
    return { hosts, participants };
  }

  #currentView(): MeshBatchView {
    const committed = this.store.stateView();
    if (committed) return committed.view;
    const snapshot = this.store.stateToken({ fresh: true });
    const get = (key: string): MeshStateEntry | undefined => this.store.get(key, { snapshot });
    return {
      get,
      listAll: (prefix) => this.store.listAll(prefix, { snapshot }),
      version: (key) => get(key)?.version ?? 0,
    };
  }

  #leaseEffectIds(entries: readonly MeshStateEntry[]): string[] {
    const ids = new Set<string>();
    for (const entry of entries) {
      const row = entry.value;
      if (!isObject(row) || (row.kind !== "lease" && row.kind !== "unlease") || !isObject(row.payload) ||
        typeof row.payload.id !== "string" || row.payload.id.length === 0) continue;
      ids.add(row.payload.id);
    }
    return [...ids];
  }

  #candidateLeaseCustodyIds(presence: BridgeMirrorPresence, hostEntries: readonly MeshStateEntry[],
    handoffs: readonly MeshStateEntry[], leases: ReadonlyMap<string, FabricHostLease>,
    outboxEntries: readonly MeshStateEntry[], scope?: ReadonlySet<string>): string[] {
    const identifiers = new Set<string>();
    const ids = new Set<string>();
    let hasHandoff = false;
    const add = (set: Set<string>, value: unknown): void => { if (typeof value === "string" && value.length > 0) set.add(value); };
    const relevant = (...values: unknown[]): boolean => scope === undefined || values.some(value => typeof value === "string" && scope.has(value));
    for (const entry of outboxEntries) {
      const row = entry.value;
      if (!isObject(row) || !isObject(row.payload) || !relevant(row.payload.id, row.payload.rootId, row.payload.identityId)) continue;
      if (row.kind === "lease" || row.kind === "unlease") add(ids, row.payload.id);
    }
    for (const { record } of presence.hosts) {
      if (!relevant(record.id, record.identity.id, record.rootId)) continue;
      add(identifiers, record.id); add(identifiers, record.identity.id); add(identifiers, record.rootId);
      add(ids, record.id);
    }
    for (const participant of presence.participants) {
      if (!relevant(participant.id, participant.ownerHostId, participant.ownerIdentityId, participant.rootId)) continue;
      add(identifiers, participant.id); add(identifiers, participant.ownerHostId);
      add(identifiers, participant.ownerIdentityId); add(identifiers, participant.rootId);
      add(ids, participant.ownerHostId);
    }
    for (const entry of hostEntries) {
      const host = hostOf(entry.key, entry.value);
      if (!host || mirrorLinkOf(entry.value) !== this.peer || !relevant(host.id, host.identity.id, host.rootId)) continue;
      add(ids, host.id); add(identifiers, host.id); add(identifiers, host.identity.id); add(identifiers, host.rootId);
    }
    for (const entry of handoffs) {
      if (!isObject(entry.value) || typeof entry.value.id !== "string" || !relevant(entry.value.id)) continue;
      hasHandoff = true;
      add(ids, entry.value.id); add(identifiers, entry.value.id);
    }
    for (const lease of leases.values()) {
      if ((hasHandoff && scope === undefined) || relevant(lease.id, lease.identityId, lease.rootId) ||
        identifiers.has(lease.id) || identifiers.has(lease.identityId) || identifiers.has(lease.rootId)) ids.add(lease.id);
    }
    return [...ids];
  }

  async #withLeaseCustody<T>(ids: Iterable<string>, operation: () => T | Promise<T>): Promise<T> {
    const sorted = [...new Set(ids)].filter(id => id.length > 0).sort();
    const previousIds = this.#leaseCustodyIds;
    const previousVersions = this.#expectedLeaseVersions;
    this.#leaseCustodyIds = new Set([...(previousIds ?? []), ...sorted]);
    this.#expectedLeaseVersions = undefined;
    const acquire = (index: number): Promise<T> => index >= sorted.length
      ? Promise.resolve(operation())
      : this.store.leaseCustody(hostLeasePath(this.store.root, sorted[index]!), () => acquire(index + 1), 0);
    try { return await acquire(0); }
    finally {
      this.#leaseCustodyIds = previousIds;
      this.#expectedLeaseVersions = previousVersions;
    }
  }

  #scheduleLeaseEffects(plan: CommitOutboxPlan, custodyIds: readonly string[], expectedVersions: ReadonlyMap<string, number>,
    snapshot: ReadonlyMap<string, FabricHostLease>, generations: ReadonlyMap<string, string>, halted: () => boolean,
    allowedLeaseCount?: number): void {
    const work = this.#leaseEffectsTail.then(async () => {
      await this.#mirrorIdle;
      await new Promise<void>(resolve => setImmediate(resolve));
    }).then(async () => {
      await this.#withLeaseCustody(custodyIds, async () => {
        if ([...generations].some(([id, generation]) => hostLeaseGeneration(this.store.root, id) !== generation)) return;
        if (halted()) return;
        const view = this.#currentView();
        this.#leaseSnapshotsByView.set(view, snapshot);
        const previousVersions = this.#expectedLeaseVersions;
        let leaseEffects = 0;
        const effectHalted = allowedLeaseCount === undefined ? halted : () => {
          if (halted()) return true;
          if (leaseEffects < allowedLeaseCount) { leaseEffects += 1; return false; }
          return true;
        };
        try {
          this.#expectedLeaseVersions = expectedVersions;
          await plan.runAsync(view, effectHalted);
        } finally { this.#expectedLeaseVersions = previousVersions; }
      });
      await this.#outbox.retire();
    });
    this.#leaseEffectsTail = work.catch(() => undefined);
  }

  async #recoverOutbox(entries: readonly MeshStateEntry[], halted: () => boolean, revalidate: () => void,
    leaseSnapshot: ReadonlyMap<string, FabricHostLease>): Promise<boolean> {
    await this.#leaseEffectsTail;
    const ids = this.#leaseEffectIds(entries);
    if (ids.length === 0) return false;
    const plan = this.#outbox.plan();
    const replayed = await this.#withLeaseCustody(ids, async () => {
      if (halted()) return false;
      revalidate();
      const base = this.#currentView();
      const allowedKeys = new Set(entries.map(entry => entry.key));
      const selected = base.listAll(this.#outbox.prefix).filter(entry => allowedKeys.has(entry.key));
      const selectedByKey = new Map(selected.map(entry => [entry.key, entry]));
      const view: MeshBatchView = {
        get: (key) => selectedByKey.get(key) ?? base.get(key),
        listAll: (prefix) => prefix === this.#outbox.prefix ? selected : base.listAll(prefix),
        version: (key) => base.version(key),
      };
      this.#leaseSnapshotsByView.set(view, leaseSnapshot);
      if (this.deferLeaseInstalls) await plan.replayAsync(view);
      else plan.replay(view);
      return true;
    });
    if (replayed) await this.#outbox.retire();
    return replayed;
  }

  async #mirror(presence: BridgeMirrorPresence, final: boolean, observedAt?: number, fromHub = false,
    processedEffects = new Set<string>(), recoveryAlreadyAttempted = false): Promise<void> {
    if (this.#mirrorDepth === 0) await this.#leaseEffectsTail;
    if (this.#mirrorDepth === 0) this.#mirrorIdle = new Promise<void>(resolve => { this.#resolveMirrorIdle = resolve; });
    this.#mirrorDepth += 1;
    try { await this.#mirrorPass(presence, final, observedAt, fromHub, processedEffects, recoveryAlreadyAttempted); }
    finally {
      this.#mirrorDepth -= 1;
      if (this.#mirrorDepth === 0) { this.#resolveMirrorIdle?.(); this.#resolveMirrorIdle = undefined; }
    }
  }

  async #mirrorPass(presence: BridgeMirrorPresence, final: boolean, observedAt?: number, fromHub = false,
    processedEffects = new Set<string>(), recoveryAlreadyAttempted = false): Promise<void> {
    await this.store.prepareLock();
    const halted = (): boolean => this.#fenced && !final;
    const now = this.now();
    // A host live when the snapshot was requested stays admissible: a pass (slow presence read,
    // transport, queueing) can outlast a 15 s source lease's remaining life, which dropped every
    // root of such a spoke on every pass (smarty-dev#6477). Never later than now. A non-finite
    // observedAt counts as now: NaN would admit every lapsed host and -Infinity would resurrect
    // one for a full lease (independent review, P2).
    const liveAt = typeof observedAt === "number" && Number.isFinite(observedAt) ? Math.min(now, observedAt) : now;
    const partial = presenceIsPartial(presence);
    // Stamp the existing mirror lease with LOCAL writer evidence, outside state custody.
    // A peer's claimed pid/host is never used, including for one-hop re-exports.
    const bridgeProcess = this.monitorLease ? await (this.#leaseProcess ??= (async () => {
      const host = readPhysicalHostIdentity();
      const incarnation = await ownProcessIncarnation();
      return host && incarnation ? { ...host, link: this.peer, pid: process.pid, incarnation } : undefined;
    })()) : undefined;
    let leases: BridgeLease[] = [];
    let removed: BridgeUnlease[] = [];
    // One idempotency key per host: a later lease write or removal supersedes an earlier one.
    const effects = (): CommitOutboxEffect[] => [
      ...leases.map((lease) => ({ kind: "lease", key: `lease:${lease.id}`, payload: lease })),
      ...removed.map((gone) => ({ kind: "unlease", key: `lease:${gone.id}`, payload: gone })),
    ];
    // A trusted hub can carry a stale projection from another link. That projection is
    // non-authoritative: keep the destination's newer mirror and let unrelated roots in this
    // same pass proceed. The set spans plan retries so one root produces one warning per pass.
    const skippedRoots = new Set<string>();
    const preservedRoots = new Set<string>();
    // Match every complete lease value consulted by this presence. A collision can resolve an
    // incoming identifier through a lease to a root that is not incoming, so the consulted set
    // includes lease ids, identity ids and root ids that touch any raw claim.
    const consultedLeaseIdentifiers = new Set<string>();
    const rememberLeaseIdentifier = (value: unknown): void => {
      if (typeof value === "string") consultedLeaseIdentifiers.add(value);
    };
    for (const { record } of presence.hosts) {
      rememberLeaseIdentifier(record.id);
      rememberLeaseIdentifier(record.identity.id);
      rememberLeaseIdentifier(record.rootId);
    }
    for (const participant of presence.participants) {
      rememberLeaseIdentifier(participant.id);
      rememberLeaseIdentifier(participant.ownerHostId);
      rememberLeaseIdentifier(participant.ownerIdentityId);
      rememberLeaseIdentifier(participant.rootId);
    }
    const leaseIncarnationDigest = (leases: ReadonlyMap<string, FabricHostLease>, includeAll = false,
      identifiers: ReadonlySet<string> = consultedLeaseIdentifiers): string => contentDigest(
      [...leases.values()].filter(lease => includeAll || identifiers.has(lease.id) ||
        identifiers.has(lease.identityId) || identifiers.has(lease.rootId))
        .sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0),
    );
    // Recompute only the collision protection from the raw records and current leases. The
    // commit-time check uses the entries captured by the early plan, which stateUnchangedSince
    // proves are still current, so it does not perform a topology scan while holding state.db.
    const collisionProtection = (
      hostEntries: readonly MeshStateEntry[], participantEntries: readonly MeshStateEntry[],
      participantFiles: ReturnType<typeof participantFileSnapshot>, leases: ReadonlyMap<string, FabricHostLease>,
    ): string[] => {
      const rootsByIdentifier = new Map<string, Set<string>>();
      const incomingRootsByIdentifier = new Map<string, Set<string>>();
      const addRoot = (map: Map<string, Set<string>>, identifier: unknown, rootId: unknown): void => {
        if (typeof identifier !== "string" || typeof rootId !== "string") return;
        const roots = map.get(identifier) ?? new Set<string>();
        roots.add(rootId);
        map.set(identifier, roots);
      };
      const addHostRoots = (map: Map<string, Set<string>>, value: unknown): void => {
        if (!isObject(value) || typeof value.id !== "string" || typeof value.rootId !== "string" || !isIdentity(value.identity)) return;
        const identity = value.identity;
        addRoot(map, value.id, value.rootId);
        addRoot(map, identity.id, value.rootId);
        addRoot(map, value.rootId, value.rootId);
      };
      for (const entry of hostEntries) addHostRoots(rootsByIdentifier, entry.value);
      for (const lease of leases.values()) {
        addRoot(rootsByIdentifier, lease.id, lease.rootId);
        addRoot(rootsByIdentifier, lease.identityId, lease.rootId);
        addRoot(rootsByIdentifier, lease.rootId, lease.rootId);
      }
      for (const entry of [...participantEntries, ...participantFiles.entries]) {
        const participant = participantOf(entry.key, entry.value);
        if (!participant) continue;
        addRoot(rootsByIdentifier, participant.id, participant.rootId);
        addRoot(rootsByIdentifier, participant.ownerHostId, participant.rootId);
        addRoot(rootsByIdentifier, participant.ownerIdentityId, participant.rootId);
        addRoot(rootsByIdentifier, participant.rootId, participant.rootId);
      }
      for (const { record } of presence.hosts) {
        addRoot(incomingRootsByIdentifier, record.id, record.rootId);
        addRoot(incomingRootsByIdentifier, record.identity.id, record.rootId);
        addRoot(incomingRootsByIdentifier, record.rootId, record.rootId);
      }
      const rootsTouchedBy = (claimedRoot: unknown, identifiers: readonly unknown[]): Set<string> => {
        const touched = new Set<string>();
        const addOwners = (identifier: unknown): void => {
          if (typeof identifier !== "string") return;
          for (const root of rootsByIdentifier.get(identifier) ?? []) touched.add(root);
          for (const root of incomingRootsByIdentifier.get(identifier) ?? []) touched.add(root);
        };
        // The claimed root is itself an identifier: a malformed frame may claim F while the
        // persisted host keyed F belongs to root G. Resolve that owner before any skip or
        // omission decision, just like the host/identity and participant-owner identifiers.
        addOwners(claimedRoot);
        if (typeof claimedRoot === "string") touched.add(claimedRoot);
        for (const identifier of identifiers) addOwners(identifier);
        return touched;
      };
      const protectionRoots = new Set<string>();
      const protect = (touched: Set<string>, requireCollision: boolean): void => {
        if (requireCollision && touched.size <= 1) return;
        for (const root of touched) protectionRoots.add(root);
      };
      for (const { record } of presence.hosts) {
        protect(rootsTouchedBy(record.rootId, [record.id, record.identity.id]), true);
      }
      for (const participant of presence.participants) {
        protect(rootsTouchedBy(participant.rootId, [participant.id, participant.ownerHostId, participant.ownerIdentityId]), true);
      }
      return [...protectionRoots].sort();
    };
    // The pass's plan on one state view: admission, reservations and the CAS versions. undefined: halted.
    const compute = (view: MeshBatchView, participantFiles: ReturnType<typeof participantFileSnapshot>,
      leaseSnapshot: ReadonlyMap<string, FabricHostLease>):
      { ops: MeshBatchOperation[]; leases: BridgeLease[]; removed: BridgeUnlease[];
        judged: { leases: ReadonlyMap<string, FabricHostLease>; now: number } } | undefined => {
        const leases: BridgeLease[] = [];
        const removed: BridgeUnlease[] = [];
        const hostEntries = view.listAll(HOST_PREFIX);
        const participantEntries = view.listAll(PARTICIPANT_PREFIX);
        const scan = this.#scan(hostEntries, [...participantEntries, ...participantFiles.entries], view.listAll(BRIDGE_HANDOFF_PREFIX), leaseSnapshot);
        const own = new Set(scan.presence.reserved);
        const protectionRoots = new Set(collisionProtection(hostEntries, participantEntries, participantFiles, scan.leases));
        for (const root of protectionRoots) skippedRoots.add(root);
        // Classify whole root groups only after all claimed/current-owner/participant-owner roots
        // are resolved. No array order may let a second host bypass collision preservation.
        const rootCounts = new Map<string, number>();
        for (const { record, expiresAt, observedAt: sourceObservedAt } of presence.hosts) {
          const nonRoot = isNativeNonRootHost(record);
          if (!nonRoot) rootCounts.set(record.rootId, (rootCounts.get(record.rootId) ?? 0) + 1);
          // Runs before recovery, planning, participant writes and omission deletion in BOTH
          // directions (remote presence replies are raw JSON, not parsed by presenceArg).
          // Missing observedAt alone is the legacy unsampled protocol, not a malformed sample.
          if (!nonRoot && remoteHostOf(record) === undefined && (!isAdmissibleRecord("host", record) || !Number.isFinite(expiresAt) ||
            (sourceObservedAt !== undefined && !Number.isFinite(sourceObservedAt)))) skippedRoots.add(record.rootId);
        }
        for (const [rootId, count] of rootCounts) if (count > 1) skippedRoots.add(rootId);
        // Same-peer mirrors are intentionally absent from reservations, but still prove a root's
        // incarnation across new link objects. Lease authority also survives a missing state row.
        const incarnations = new Map<string, Array<{ startedAt?: number; incarnationToken?: string }>>();
        const superseded = new Map<string, BridgeSupersededHistory[]>();
        const currentTokens = new Map<string, string[]>();
        const remember = (rootId: string, record: { startedAt?: unknown; incarnationToken?: unknown;
          bridgeSupersededTokens?: unknown; bridgeSupersededTokenSeen?: unknown }): void => {
          const known = incarnations.get(rootId) ?? [];
          known.push(incarnationOf(record));
          incarnations.set(rootId, known);
          // Union all local evidence before bounding, including an older authority's history.
          superseded.set(rootId, [...(superseded.get(rootId) ?? []),
            { tokens: record.bridgeSupersededTokens, seen: record.bridgeSupersededTokenSeen }]);
          const token = incarnationOf(record).incarnationToken;
          if (token !== undefined) currentTokens.set(rootId, [...new Set([...(currentTokens.get(rootId) ?? []), token])]);
        };
        for (const entry of hostEntries) {
          const held = hostOf(entry.key, entry.value);
          // The remote's history is not evidence. Only this link's own persisted root mirror counts.
          if (held && !isNonRootHostRecord(held) && mirrorLinkOf(entry.value) === this.peer) remember(held.rootId, held);
        }
        for (const lease of scan.leases.values()) {
          if (!isNonRootHostId(lease.id, lease.rootId)) remember(lease.rootId, lease);
        }
        // Mark protected direct roots before any host/participant can target their keys under
        // a different claimed root. Protection cannot depend on presence array order.
        for (const { record } of presence.hosts) {
          if (!isNativeNonRootHost(record) && remoteHostOf(record) === undefined &&
            incarnationOf(record).incarnationToken === undefined && ((currentTokens.get(record.rootId)?.length ?? 0) > 0 ||
              mergeBridgeSupersededHistory(superseded.get(record.rootId) ?? []).tokens.length > 0)) preservedRoots.add(record.rootId);
        }
        const wanted = new Map<string, { value: Record<string, unknown>; identity: MeshIdentity }>();
        const hosts = new Map<string, FabricHostRecord>();
        for (const { record, expiresAt, observedAt: sourceObservedAt } of presence.hosts) {
          const relayed = remoteHostOf(record) !== undefined;
          const nonRoot = isNativeNonRootHost(record);
          if (relayed && (!fromHub || record.bridgeVia !== this.peer || record.remoteHost === this.peer)) continue;
          // A preflight skip remains a skip after replay or a plan retry, even if authority moved.
          if (!nonRoot && skippedRoots.has(record.rootId)) continue;
          if (own.has(record.id) || own.has(record.identity.id) || own.has(record.rootId)) continue;
          const prior = view.get(keyFor(HOST_PREFIX, record.id))?.value;
          if (isObject(prior) && typeof prior.rootId === "string" && prior.rootId !== record.rootId &&
            (preservedRoots.has(prior.rootId) || skippedRoots.has(prior.rootId))) {
            if (!nonRoot) skippedRoots.add(record.rootId);
            continue;
          }
          if (nonRoot) {
            if (skippedRoots.has(record.rootId) || preservedRoots.has(record.rootId) || !rootCounts.has(record.rootId)) continue;
            // Child/resident host rows are useful to the remote directory and reapers, but never
            // represent a root, participant owner or bridge route. Keep their own identity and
            // liveness projection without inheriting the main root's incarnation history.
            const sampled = typeof sourceObservedAt === "number" && Number.isFinite(sourceObservedAt);
            if (!Number.isFinite(expiresAt) || expiresAt <= (sampled ? sourceObservedAt : liveAt) ||
              (sourceObservedAt !== undefined && !sampled)) continue;
            const origin = sampled ? liveAt + Math.min(BRIDGE_LEASE_MS, expiresAt - sourceObservedAt) : expiresAt;
            const until = sampled ? origin
              : Math.min(origin + Math.min(now - liveAt, BRIDGE_LEASE_MS), now + BRIDGE_LEASE_MS);
            if (!sampled && until <= now) continue;
            const renewedAt = sampled ? record.updatedAt : Math.min(record.updatedAt, now);
            const incarnation = incarnationOf(record);
            const { bridgeSupersededTokens: _tokens, bridgeSupersededTokenSeen: _seen, ...fields } = record;
            const admitted = { ...fields, ...incarnation };
            const mirror: FabricMirrorLease | undefined = sampled ? { remoteHost: this.peer, observedAt: liveAt,
              sourceObservedAt, sourceUpdatedAt: record.updatedAt, sourceExpiresAt: expiresAt,
              ...(bridgeProcess ? { bridgeProcess } : {}) } : undefined;
            wanted.set(keyFor(HOST_PREFIX, record.id), {
              value: { ...admitted, updatedAt: now, expiresAt: until, remoteHost: this.peer,
                ...(sampled ? { mirrorLease: 1 } : {}) },
              identity: record.identity,
            });
            leases.push({ id: record.id, rootId: record.rootId, identityId: record.identity.id, ...incarnation,
              updatedAt: renewedAt, expiresAt: sampled ? origin : until, ...(mirror ? { mirrorLease: 1, mirror } : {}) });
            continue;
          }
          // Refuse explicit stale native/direct presence rather than omitting it: omission would
          // delete the newer mirror, its participants and its lease in the removal pass below.
          // Check before liveness filtering too: an expired stale frame proves no withdrawal.
          const knownHistory = mergeBridgeSupersededHistory(superseded.get(record.rootId) ?? []);
          const tokenlessAuthority = incarnationOf(record).incarnationToken === undefined &&
            ((currentTokens.get(record.rootId)?.length ?? 0) > 0 || knownHistory.tokens.length > 0 || preservedRoots.has(record.rootId));
          let retained: FabricHostRecord | undefined;
          if (!relayed && tokenlessAuthority) {
            // Tokenless native presence supplies liveness, NEVER replacement owner evidence.
            // Preserve first, including expired rows/participants and unusable source samples.
            // These decisions only strengthen across early plans, replay and recomputation.
            preservedRoots.add(record.rootId);
            const entry = view.get(keyFor(HOST_PREFIX, record.id));
            const held = entry && hostOf(entry.key, entry.value);
            const token = held && incarnationOf(held).incarnationToken;
            const tokens = currentTokens.get(record.rootId) ?? [];
            if (!held || held.remoteHost !== this.peer || held.bridgeVia !== undefined ||
              held.id !== record.id || held.rootId !== record.rootId || held.identity.id !== record.identity.id ||
              held.id !== held.rootId || held.id !== held.identity.id || entry!.updatedBy.id !== held.identity.id ||
              !Number.isFinite(held.startedAt) || !Number.isFinite(held.updatedAt) || !Number.isFinite(held.expiresAt) ||
              token === undefined || tokens.length !== 1 || tokens[0] !== token ||
              knownHistory.tokens.includes(token)) {
              skippedRoots.add(record.rootId);
              continue;
            }
            retained = held;
          }
          if ((relayed && tokenlessAuthority) || olderBridgeIncarnation(record, knownHistory.tokens)) {
            // Explicit stale direct-link tokens remain a hard refusal/fence. Only a
            // structurally attributable re-export from the trusted hub may be skipped:
            // omitting it preserves the already-held mirror instead of treating absence as
            // withdrawal. K05 identity and ownership checks above remain unchanged.
            if (relayed && fromHub && record.bridgeVia === this.peer && record.remoteHost !== this.peer) {
              // The Set is intentionally scoped to this mirror pass; compute can run more than
              // once while the SQLite plan is retried.
              const firstSkip = !skippedRoots.has(record.rootId);
              skippedRoots.add(record.rootId);
              if (firstSkip) console.warn(`bridge ${this.peer}: skipped stale relayed root ${record.rootId}; preserving its mirror`);
              continue;
            }
            throw new BridgeStaleIncarnationError(record.rootId, record.id);
          }
          // Source expiry and age are meaningful on the SOURCE clock only (#10068).
          // Keep legacy projection deadlines for older readers, but give current readers the
          // successful per-host observation. A delayed commit never makes that snapshot fresh.
          const sampled = typeof sourceObservedAt === "number" && Number.isFinite(sourceObservedAt);
          if (!Number.isFinite(expiresAt) || expiresAt <= (sampled ? sourceObservedAt : liveAt) ||
            (retained && (!Number.isFinite(record.updatedAt) || (sourceObservedAt !== undefined && !sampled)))) {
            // Malformed direct samples were skipped in preflight. A finite expired
            // explicit owner sample is withdrawal; tokenless liveness cannot withdraw it.
            if (!relayed && incarnationOf(record).incarnationToken === undefined) skippedRoots.add(record.rootId);
            continue;
          }
          // A sampled origin's remaining life is a SOURCE-clock duration, anchored at liveAt (this
          // clock, before the source read it), so liveAt + remaining never overstates the origin.
          // Never extend sampled authority for pass latency: retain an expired projection for
          // routing attribution, but readers judge its translated deadline bridge-stale. Older
          // unsampled peers retain the bounded latency allowance (smarty-dev#6477).
          const origin = sampled ? liveAt + Math.min(BRIDGE_LEASE_MS, expiresAt - sourceObservedAt) : expiresAt;
          const until = sampled ? origin
            : Math.min(origin + Math.min(now - liveAt, BRIDGE_LEASE_MS), now + BRIDGE_LEASE_MS);
          if (!sampled && until <= now) {
            if (!relayed && incarnationOf(record).incarnationToken === undefined) skippedRoots.add(record.rootId);
            continue;
          }
          const renewedAt = sampled ? record.updatedAt : Math.min(record.updatedAt, now);
          const mirror: FabricMirrorLease | undefined = sampled ? { remoteHost: relayed ? record.remoteHost! : this.peer, observedAt: liveAt,
            sourceObservedAt, sourceUpdatedAt: record.updatedAt, sourceExpiresAt: expiresAt,
            ...(bridgeProcess ? { bridgeProcess } : {}) } : undefined;
          const authority = retained ?? record;
          const incarnation = bridgeIncarnationWithFloor(authority, incarnations.get(record.rootId) ?? []);
          const token = incarnation.incarnationToken;
          const priorTokens = token === undefined ? []
            : (currentTokens.get(record.rootId) ?? []).filter(held => held !== token);
          const history = mergeBridgeSupersededHistory(superseded.get(record.rootId) ?? [], priorTokens);
          const { bridgeSupersededTokens: _tokens, bridgeSupersededTokenSeen: _seen, ...recordFields } = authority;
          const admitted = { ...recordFields, ...incarnation,
            ...(history.tokens.length > 0 ? { bridgeSupersededTokens: history.tokens } : {}),
            ...(Object.keys(history.seen).length > 0 ? { bridgeSupersededTokenSeen: history.seen } : {}) };
          hosts.set(record.id, admitted);
          wanted.set(keyFor(HOST_PREFIX, record.id), {
            value: { ...admitted, updatedAt: now, expiresAt: until, remoteHost: relayed ? record.remoteHost : this.peer,
              ...(sampled ? { mirrorLease: 1 } : {}) },
            identity: authority.identity,
          });
          // The lease carries the same age floor as the host, including lease-only authority.
          leases.push({ id: record.id, rootId: record.rootId, identityId: record.identity.id, ...incarnation,
            ...(history.tokens.length > 0 ? { bridgeSupersededTokens: history.tokens } : {}),
            ...(Object.keys(history.seen).length > 0 ? { bridgeSupersededTokenSeen: history.seen } : {}),
            // Keep the bounded sources' first-seen evidence until the file effect completes;
            // otherwise an evicted duplicate could reenter from disk with a later ordinal.
            historySeenFloor: history.firstSeen,
            updatedAt: renewedAt, expiresAt: until, ...(mirror ? { mirrorLease: 1, mirror } : {}) });
        }
        for (const participant of presence.participants) {
          if (preservedRoots.has(participant.rootId) || skippedRoots.has(participant.rootId)) continue;
          const owner = hosts.get(participant.ownerHostId);
          if (!owner || participant.kind !== "root" || own.has(participant.id)) continue;
          if (remoteHostOf(participant) !== undefined && (!fromHub || participant.bridgeVia !== this.peer ||
              participant.remoteHost !== owner.remoteHost)) continue;
          if (owner.id !== participant.id || owner.identity.id !== participant.ownerIdentityId || owner.rootId !== participant.rootId) continue;
          wanted.set(keyFor(PARTICIPANT_PREFIX, participant.id), {
            value: { ...participant, remoteHost: participant.remoteHost ?? this.peer },
            identity: owner.identity,
          });
        }
        const mirrored = new Map([...hostEntries, ...participantEntries].map((entry) => [entry.key, entry]));
        const ops: MeshBatchOperation[] = [];
        for (const [key, { value, identity }] of wanted) {
          const existing = mirrored.get(key);
          // An unrelated participant may not overwrite a protected row by claiming its key
          // with a different root/owner. Retained hosts alone may update their own liveness.
          if (existing && isObject(existing.value) && typeof existing.value.rootId === "string" &&
            (preservedRoots.has(existing.value.rootId) || skippedRoots.has(existing.value.rootId)) &&
            (key.startsWith(PARTICIPANT_PREFIX) || existing.value.rootId !== value.rootId)) continue;
          // Never replace a native record, or another bridge's mirror (anti-spoofing); a native may be
          // only in its own file (smarty-dev#2004).
          if (existing && mirrorLinkOf(existing.value) !== this.peer) continue;
          if (participantFiles.present(key)) continue;
          if (
            existing && isObject(existing.value) && settled(existing.value) === settled(value) &&
            contentDigest(existing.updatedBy) === contentDigest(identity) &&
            (key.startsWith(HOST_PREFIX)
              ? typeof existing.value.updatedAt !== "number" || now - existing.value.updatedAt < STATE_LEASE_RENEW_MS
              : existing.value.updatedAt === value.updatedAt)
          ) continue;
          if (halted()) return undefined;
          ops.push({ kind: "put", key, value, identity, ifVersion: view.version(key), onConflict: "skip" });
        }
        if (!partial) for (const [key, { value, version }] of mirrored) {
          if (halted()) return undefined;
          if (wanted.has(key) || mirrorLinkOf(value) !== this.peer) continue;
          // Skipped roots and retained-owner renewals preserve existing participant bytes.
          // Neither unusable liveness nor ignored tokenless input is withdrawal evidence.
          if (isObject(value) && typeof value.rootId === "string" &&
            (skippedRoots.has(value.rootId) || preservedRoots.has(value.rootId))) continue;
          if (key.startsWith(HOST_PREFIX) && isObject(value) && typeof value.id === "string") {
            const gone = hostOf(key, value);
            removed.push(gone
              ? { key, id: gone.id, rootId: gone.rootId, identityId: gone.identity.id, ...incarnationOf(gone) }
              : { key, id: value.id });
          }
          ops.push({ kind: "delete", key, ifVersion: version, onConflict: "skip" });
        }
        // A handoff a native voided goes in this commit: the void lasts if that native leaves again.
        if (!partial) for (const entry of scan.voided) ops.push({ kind: "delete", key: entry.key, ifVersion: entry.version, onConflict: "skip" });
        return { ops, leases, removed, judged: { leases: scan.leases, now: scan.now } };
    };
    let recoveryAttempted = recoveryAlreadyAttempted;
    for (let attempt = 0; ; attempt += 1) {
      ensureParticipantDirectoryWatch(this.store.root);
      const filesStamp = meshDirectoryStamp(this.store.root, "participants");
      const filesGenerationBefore = participantDirectoryGeneration(this.store.root);
      const files = participantFileSnapshot(this.store.root);
      const filesGeneration = participantDirectoryGeneration(this.store.root);
      if (filesGenerationBefore !== filesGeneration) continue;
      const leaseSnapshot = readHostLeases(this.store.root);
      const leaseGeneration = hostLeaseCacheGeneration(this.store.root);
      const early = this.store.stateBackend === "sqlite" ? this.store.stateView()
        : attempt < MIRROR_PLAN_ATTEMPTS - 1 ? this.store.stateView() : undefined;
      if (this.store.stateBackend === "sqlite" && !early) throw new Error("SQLite bridge mirror requires a revisioned state view");
      const snapshotView = early?.view ?? this.#currentView();
      const handoffEntries = snapshotView.listAll(BRIDGE_HANDOFF_PREFIX);
      const judgesLeaseLiveness = handoffEntries.length > 0;
      const planned = halted() ? undefined : compute(snapshotView, files, leaseSnapshot);
      if (halted()) break;
      if (!planned) break;
      const hostEntries = snapshotView.listAll(HOST_PREFIX);
      const participantEntries = snapshotView.listAll(PARTICIPANT_PREFIX);
      const outboxEntries = this.store.listAll(this.#outbox.prefix, { fresh: true });
      const groups = new Map<string, MeshBatchOperation[]>();
      for (const op of planned.ops) {
        const value = op.kind === "put" ? op.value : snapshotView.get(op.key)?.value;
        const group = isObject(value) && typeof value.rootId === "string" ? value.rootId
          : isObject(value) && typeof value.id === "string" ? value.id : op.key;
        const groupOps = groups.get(group) ?? [];
        groupOps.push(op);
        groups.set(group, groupOps);
      }
      const batchOps: MeshBatchOperation[] = [];
      const batchRoots = new Set<string>();
      for (const [root, groupOps] of groups) {
        if (batchOps.length > 0 && batchOps.length + groupOps.length > MIRROR_BATCH_MAX_OPS) break;
        const count = Math.min(groupOps.length, MIRROR_BATCH_MAX_OPS - batchOps.length);
        batchRoots.add(root);
        batchOps.push(...groupOps.slice(0, count));
        if (count < groupOps.length || batchOps.length >= MIRROR_BATCH_MAX_OPS) break;
      }
      const batchOpKeys = new Set(batchOps.map(op => op.key));
      const allOpKeys = new Set(planned.ops.map(op => op.key));
      const leaseCandidates = planned.leases.filter(lease => !processedEffects.has(`lease:${lease.id}`) &&
        (batchOpKeys.has(keyFor(HOST_PREFIX, lease.id)) || !allOpKeys.has(keyFor(HOST_PREFIX, lease.id))));
      const removedCandidates = planned.removed.filter(gone => !processedEffects.has(`unlease:${gone.id}`) && batchOpKeys.has(gone.key));
      const leaseBatch = leaseCandidates.slice(0, MIRROR_BATCH_MAX_EFFECTS);
      const removedBatch = removedCandidates.slice(0, MIRROR_BATCH_MAX_EFFECTS - leaseBatch.length);
      const batchScope = new Set(batchRoots);
      const addScope = (value: unknown): void => { if (typeof value === "string" && value.length > 0) batchScope.add(value); };
      for (const op of batchOps) {
        const value = op.kind === "put" ? op.value : snapshotView.get(op.key)?.value;
        if (!isObject(value)) continue;
        addScope(value.rootId); addScope(value.id); addScope(value.ownerHostId); addScope(value.ownerIdentityId);
        if (isIdentity(value.identity)) addScope(value.identity.id);
      }
      for (const root of batchRoots) {
        for (const { record } of presence.hosts) if (record.rootId === root) {
          addScope(record.id); addScope(record.identity.id); addScope(record.rootId);
        }
        for (const participant of presence.participants) if (participant.rootId === root) {
          addScope(participant.id); addScope(participant.ownerHostId); addScope(participant.ownerIdentityId); addScope(participant.rootId);
        }
        for (const entry of hostEntries) {
          const host = hostOf(entry.key, entry.value);
          if (host?.rootId === root) { addScope(host.id); addScope(host.identity.id); addScope(host.rootId); }
        }
        for (const entry of participantEntries) if (isObject(entry.value) && entry.value.rootId === root) {
          addScope(entry.value.id); addScope(entry.value.ownerHostId); addScope(entry.value.ownerIdentityId); addScope(entry.value.rootId);
        }
      }
      for (const lease of leaseBatch) { addScope(lease.id); addScope(lease.rootId); addScope(lease.identityId); }
      for (const gone of removedBatch) { addScope(gone.id); addScope(gone.rootId); addScope(gone.identityId); }
      const custodyIds = this.#candidateLeaseCustodyIds(presence, hostEntries, handoffEntries, leaseSnapshot, outboxEntries, batchScope);
      const more = planned.ops.length > batchOps.length || leaseCandidates.length > leaseBatch.length ||
        removedCandidates.length > removedBatch.length;
      let preparedLeaseGeneration = leaseGeneration;
      const externalInputsStable = (identifiers: ReadonlySet<string> = consultedLeaseIdentifiers): boolean => {
        if (participantDirectoryGeneration(this.store.root) !== filesGeneration ||
          meshDirectoryStamp(this.store.root, "participants") !== filesStamp) return false;
        if (process.platform !== "win32" && hostLeaseCacheGeneration(this.store.root) !== leaseGeneration) return false;
        const currentLeases = readHostLeases(this.store.root);
        const currentGeneration = hostLeaseCacheGeneration(this.store.root);
        if (process.platform !== "win32" && currentGeneration !== leaseGeneration) return false;
        preparedLeaseGeneration = currentGeneration;
        return leaseIncarnationDigest(leaseSnapshot, judgesLeaseLiveness, identifiers) ===
          leaseIncarnationDigest(currentLeases, judgesLeaseLiveness, identifiers) &&
          (!judgesLeaseLiveness || sameLeaseLiveness(leaseSnapshot, planned.judged.now, this.now(), identifiers));
      };
      if (batchOps.length === 0 && leaseBatch.length === 0 && removedBatch.length === 0) break;
      this.#skippedRoots = skippedRoots;
      try {
        if (!partial && !recoveryAttempted && outboxEntries.length > 0 && planned) {
          const recovered = await this.#recoverOutbox(outboxEntries, halted, () => {
            if (!externalInputsStable()) throw new MirrorPlanStale();
            if (early && !this.store.stateUnchangedSince(early.revision, HELD_STATE_PREFIXES)) throw new MirrorPlanStale();
            if (!compute(this.#currentView(), files, leaseSnapshot)) throw new MirrorPlanStale();
          }, leaseSnapshot);
          recoveryAttempted = true;
          if (recovered) continue;
        }

        const plan = this.#outbox.plan();
        const selectedLeases = new Set(leaseBatch.map(lease => lease.id));
        const selectedRemovals = new Set(removedBatch.map(gone => gone.key));
        if (early && batchOps.length === 0) {
          await this.#withLeaseCustody(custodyIds, () => {
            if (halted()) return;
            if (!externalInputsStable(batchScope) || !this.store.stateUnchangedSince(early.revision, HELD_STATE_PREFIXES)) {
              throw new MirrorPlanStale();
            }
            const view = this.#currentView();
            this.#leaseSnapshotsByView.set(view, leaseSnapshot);
            const result = compute(view, files, leaseSnapshot);
            if (!result || result.ops.length !== 0) throw new MirrorPlanStale();
            leases = result.leases.filter(lease => selectedLeases.has(lease.id)).map(lease => {
              const sample = lease.mirror;
              if (!sample) return lease;
              const deadline = sample.observedAt + Math.min(BRIDGE_LEASE_MS, sample.sourceExpiresAt - sample.sourceObservedAt);
              return { ...lease, expiresAt: Math.min(lease.expiresAt, deadline) };
            });
            removed = result.removed.filter(gone => selectedRemovals.has(gone.key));
            plan.stage([], effects());
            const expected = new Map<string, number>(leases.map(lease => {
              const key = keyFor(HOST_PREFIX, lease.id);
              return [key, view.version(key)] as const;
            }));
            for (const gone of removed) expected.set(gone.key, view.version(gone.key));
            const generations = new Map([...leases.map(lease => lease.id), ...removed.map(gone => gone.id)]
              .map(id => [id, hostLeaseGeneration(this.store.root, id)] as const));
            if (this.deferLeaseInstalls) {
              this.#scheduleLeaseEffects(plan, custodyIds, expected, leaseSnapshot, generations, halted,
                partial ? leases.length : undefined);
            } else {
              this.#expectedLeaseVersions = expected;
              try {
                if (!partial) plan.run(view, halted);
                else {
                  let allowedLeaseEffects = 0;
                  plan.run(view, () => {
                    if (halted()) return true;
                    if (allowedLeaseEffects < leases.length) { allowedLeaseEffects += 1; return false; }
                    return true;
                  });
                }
              } finally { this.#expectedLeaseVersions = undefined; }
            }
          });
          await this.#outbox.retire();
        } else {

        await this.#withLeaseCustody(custodyIds, async () => {
          if (!externalInputsStable(batchScope)) throw new MirrorPlanStale();
          await this.store.writeBatch({
            identity: this.#identity(),
            lockClass: "bridge",
            ops: [],
            // State admission is planned before the writer lock when SQLite can provide a revision.
            // The locked path only validates in-memory filesystem generations and recomputes from
            // the transaction view on fallback; no participant or host-lease I/O runs in prepare.
            prepare: (view) => {
              if (halted()) return plan.stage([], []);
              if (participantDirectoryGeneration(this.store.root) !== filesGeneration ||
                hostLeaseCacheGeneration(this.store.root) !== preparedLeaseGeneration) throw new MirrorPlanStale();
              let result: ReturnType<typeof compute>;
              if (early && planned && this.store.stateUnchangedSince(early.revision, HELD_STATE_PREFIXES)) {
                if (judgesLeaseLiveness && !sameLeaseLiveness(planned.judged.leases, planned.judged.now, this.now(), batchScope)) {
                  throw new MirrorPlanStale();
                }
                result = planned;
              } else if (early) throw new MirrorPlanStale();
              else result = compute(view, files, leaseSnapshot);
              if (!result) return plan.stage([], []);
              const deadlines = new Map<string, number>();
              leases = result.leases.filter(lease => selectedLeases.has(lease.id)).map(lease => {
                const sample = lease.mirror;
                if (!sample) return lease;
                const deadline = sample.observedAt + Math.min(BRIDGE_LEASE_MS, sample.sourceExpiresAt - sample.sourceObservedAt);
                deadlines.set(keyFor(HOST_PREFIX, lease.id), deadline);
                return { ...lease, expiresAt: Math.min(lease.expiresAt, deadline) };
              });
              removed = result.removed.filter(gone => selectedRemovals.has(gone.key));
              const selectedOps = result.ops.filter(op => batchOpKeys.has(op.key));
              const ops = deadlines.size === 0 ? selectedOps : selectedOps.map(op => {
                const deadline = op.kind === "put" ? deadlines.get(op.key) : undefined;
                return op.kind === "put" && deadline !== undefined && isObject(op.value)
                  ? { ...op, value: { ...op.value, expiresAt: deadline } } : op;
              });
              return plan.stage(ops, effects()).filter(op => {
                if (op.kind !== "delete" || !op.key.startsWith(this.#outbox.prefix)) return true;
                const row = view.get(op.key)?.value;
                return !isObject(row) || !isObject(row.payload) || typeof row.payload.rootId !== "string" ||
                  !skippedRoots.has(row.payload.rootId);
              });
            },
            // The outbox is outside state custody. Lease gates remain held until this awaited
            // synchronous drain finishes, so host-lease ownership and version checks stay fenced.
            commitOutbox: (committed) => {
              const expected = new Map<string, number>();
              for (const result of committed.results) if (result.applied) expected.set(result.key, result.version);
              for (const lease of leases) {
                const key = keyFor(HOST_PREFIX, lease.id);
                if (!expected.has(key)) expected.set(key, committed.view.version(key));
              }
              for (const gone of removed) if (!expected.has(gone.key)) expected.set(gone.key, committed.view.version(gone.key));
              this.#leaseSnapshotsByView.set(committed.view, leaseSnapshot);
              if (this.deferLeaseInstalls) {
                const generations = new Map([...leases.map(lease => lease.id), ...removed.map(gone => gone.id)]
                  .map(id => [id, hostLeaseGeneration(this.store.root, id)] as const));
                this.#scheduleLeaseEffects(plan, custodyIds, expected, leaseSnapshot, generations, halted,
                  partial ? leases.length : undefined);
                return;
              }
              this.#expectedLeaseVersions = expected;
              try {
                if (!partial) { plan.run(committed.view, halted); return; }
                let allowedLeaseEffects = 0;
                plan.run(committed.view, () => {
                  if (halted()) return true;
                  if (allowedLeaseEffects < leases.length) { allowedLeaseEffects += 1; return false; }
                  return true;
                });
              } finally { this.#expectedLeaseVersions = undefined; }
            },
          });
        });
        await this.#outbox.retire();
        }
        for (const lease of leaseBatch) processedEffects.add(`lease:${lease.id}`);
        for (const gone of removedBatch) processedEffects.add(`unlease:${gone.id}`);
        if (more && !halted()) {
          await this.#mirror(presence, final, observedAt, fromHub, processedEffects, true);
          return;
        }
      } catch (error) {
        if (error instanceof MirrorPlanStale) continue;
        throw error;
      } finally {
        this.#skippedRoots = undefined;
      }
      break;
    }
    if (!final) {
      this.#hasCommittedPass = true;
      // A complete committed pass ends recovery; partial passes cannot certify pending omissions.
      if (!partial) this.#recovered = true;
    }
  }

  #leaseToInstall(lease: BridgeLease, view: MeshBatchView, replay: boolean): FabricHostLease | undefined {
    const entry = view.get(keyFor(HOST_PREFIX, lease.id));
    const held = entry && mirrorLinkOf(entry.value) === this.peer ? hostOf(entry.key, entry.value) : undefined;
    if (!held || held.id !== lease.id || held.identity.id !== lease.identityId || held.rootId !== lease.rootId) return undefined;
    // Only the incarnation the lease was planned for: an overlapping writer may have installed a
    // replacement mirror (same id, root and identity, new start/token) since this batch committed,
    // and a stale effect must not overwrite its lease (pi-fabric#640 review round 2).
    if (typeof lease.startedAt !== "number" || lease.startedAt !== held.startedAt ||
      lease.incarnationToken !== held.incarnationToken) return undefined;
    const sample = lease.mirror;
    const sourceDeadline = sample ? sample.observedAt +
      Math.min(BRIDGE_LEASE_MS, sample.sourceExpiresAt - sample.sourceObservedAt) : undefined;
    if (sourceDeadline !== undefined) lease = { ...lease, expiresAt: Math.min(lease.expiresAt, sourceDeadline) };
    // Mirror planning captured one in-memory lease snapshot before state commit. Use that value
    // for replay ordering; never re-read each lease file in the install path.
    const snapshot = this.#leaseSnapshotsByView.get(view);
    const prior = snapshot ? snapshot.get(lease.id) : undefined;
    if (prior?.rootId === lease.rootId) {
      if (olderBridgeIncarnation(lease, bridgeSupersededTokensOf(prior.bridgeSupersededTokens))) return undefined;
      if (bridgeIncarnationWithFloor(lease, [prior]).startedAt !== incarnationOf(lease).startedAt) return undefined;
    }
    const sameIncarnation = prior && prior.startedAt === lease.startedAt && prior.identityId === lease.identityId &&
      prior.rootId === lease.rootId && prior.incarnationToken === lease.incarnationToken;
    const history = mergeBridgeSupersededHistory([
      { tokens: held.bridgeSupersededTokens, seen: held.bridgeSupersededTokenSeen },
      { tokens: lease.bridgeSupersededTokens, seen: lease.bridgeSupersededTokenSeen },
      { seen: lease.historySeenFloor },
      ...(prior?.rootId === lease.rootId ? [{ tokens: prior.bridgeSupersededTokens, seen: prior.bridgeSupersededTokenSeen }] : []),
    ]);
    const keepPriorLease = sameIncarnation && prior && (
      (replay && (prior.mirror?.observedAt ?? prior.expiresAt) >= (sample?.observedAt ?? lease.expiresAt)) ||
      (sample ? (prior.mirror?.observedAt ?? -Infinity) >= sample.observedAt ||
        (prior.mirror?.sourceObservedAt === sample.sourceObservedAt &&
          prior.mirror.sourceUpdatedAt === sample.sourceUpdatedAt && prior.mirror.sourceExpiresAt === sample.sourceExpiresAt)
        : prior.expiresAt > lease.expiresAt ||
          (prior.expiresAt === lease.expiresAt && prior.updatedAt >= lease.updatedAt)));
    let base = keepPriorLease ? { ...lease, ...prior } : sameIncarnation && prior && !sample ? {
      ...lease,
      updatedAt: Math.max(lease.updatedAt, prior.updatedAt),
      expiresAt: Math.max(lease.expiresAt, prior.expiresAt),
    } : lease;
    if (base.mirror) base = { ...base, expiresAt: Math.min(base.expiresAt, base.mirror.observedAt +
      Math.min(BRIDGE_LEASE_MS, base.mirror.sourceExpiresAt - base.mirror.sourceObservedAt)) };
    const { historySeenFloor: _floor, bridgeSupersededTokens: _tokens, bridgeSupersededTokenSeen: _seen, ...leaseFields } = base;
    const repaired = { ...leaseFields,
      ...(history.tokens.length > 0 ? { bridgeSupersededTokens: history.tokens } : {}),
      ...(Object.keys(history.seen).length > 0 ? { bridgeSupersededTokenSeen: history.seen } : {}) };
    const digest = contentDigest(repaired);
    if (snapshot) {
      if (prior && contentDigest(prior) === digest) {
        this.#lastWrittenLeases.set(lease.id, digest);
        return undefined;
      }
      this.#lastWrittenLeases.delete(lease.id);
    }
    if (this.#lastWrittenLeases.get(lease.id) === digest) return undefined;
    return repaired;
  }

  #applyLeaseSync(lease: BridgeLease, view: MeshBatchView, replay: boolean): void {
    const repaired = this.#leaseToInstall(lease, view, replay);
    if (!repaired) return;
    writeHostLease(this.store.root, repaired);
    this.#lastWrittenLeases.set(lease.id, contentDigest(repaired));
  }

  async #applyLeaseAsync(lease: BridgeLease, view: MeshBatchView, replay: boolean): Promise<void> {
    const repaired = this.#leaseToInstall(lease, view, replay);
    if (!repaired) return;
    await writeHostLeaseAsync(this.store.root, repaired);
    this.#lastWrittenLeases.set(lease.id, contentDigest(repaired));
  }

  async bridgedIds(after: number): Promise<BridgedIds> {
    // At the saved mark there is usually no recovery work. Do not seed sequence hints
    // by scanning a large log just to discover that its committed head is already covered.
    if (after >= (await this.latestCursor()).through) return { ids: [], through: after, done: true };
    const ids: string[] = [];
    let cursor = after;
    while (true) {
      const page = this.store.read({ after: cursor, limit: this.store.maxReadEvents });
      for (const event of page) {
        const stamp = bridgeStampOf(event);
        if ((stamp?.via ?? stamp?.from) === this.peer) ids.push(stamp!.id);
        else if (isRelayReceipt(event)) {
          const prefix = `${RELAY_RECEIPT_PREFIX}${encodeURIComponent(this.peer)}:`;
          const key = event.id.startsWith(prefix) ? event.id : event.dedupeKey;
          if (key?.startsWith(prefix)) {
            try { ids.push(decodeURIComponent(key.slice(prefix.length))); } catch { /* malformed receipt grants no recovery */ }
          }
        }
      }
      if (page.length < this.store.maxReadEvents) return { ids, through: page.at(-1)?.sequence ?? cursor, done: true };
      cursor = page.at(-1)!.sequence;
      if (ids.length >= BRIDGED_IDS_PAGE) return { ids, through: cursor, done: false };
    }
  }

}

/** An event refused at commit: its sender or target is no longer bound to this link. */
export class BridgeOwnershipError extends Error {}

export const FABRIC_BRIDGE_STALE_INCARNATION = "FABRIC_BRIDGE_STALE_INCARNATION";

/** Presence refused without changing the newer persisted root or its lease. */
export class BridgeStaleIncarnationError extends Error {
  override readonly name = "BridgeStaleIncarnationError";
  readonly code = FABRIC_BRIDGE_STALE_INCARNATION;
  readonly retryable = false;
  constructor(readonly rootId?: string, readonly hostId?: string) {
    super(`Bridge presence${rootId === undefined ? "" : ` for ${rootId}`}${hostId === undefined ? "" : ` (host ${hostId})`} has an older incarnation than persisted authority`);
  }
}

/** The shape and allow-list check a side applies to every event it is asked to publish. */
export const checkBridgePublish = (input: unknown): BridgePublish => {
  if (!isObject(input)) throw new Error("Bridge publish is not an object");
  const { topic, kind, from, to, text, data } = input;
  const principal = copyFabricPrincipal(input.principal);
  if (typeof topic !== "string" || typeof kind !== "string" || !isBridgedTopic({ topic, kind, to })) {
    throw new Error(`Bridge topic is not allowed: ${String(topic)}`);
  }
  if (!isIdentity(from)) throw new Error("Bridge publish has no sender identity");
  if (typeof to !== "string" || !to.trim()) throw new Error("Bridge publish has no recipient");
  if (text !== undefined && typeof text !== "string") throw new Error("Bridge publish text is not a string");
  if (!isObject(data) || !bridgeStampOf({ data })) throw new Error("Bridge publish has no bridge stamp");
  return {
    topic, kind, to,
    ...(principal ? { principal } : {}),
    from: { id: from.id, name: from.name, kind: from.kind, ...(typeof from.sessionId === "string" ? { sessionId: from.sessionId } : {}) },
    ...(text !== undefined ? { text } : {}),
    data: data as BridgePublish["data"],
  };
};

// ---------------------------------------------------------------------------------------------
// Stdio transport: one JSON request or response per line.

type RpcOp = "hello" | "latestSequence" | "latestCursor" | "tail" | "read" | "presence" | "publish" | "publishReceipt" | "publishBatch" | "publishReceiptTimes" | "mirror" | "bridgedIds";

interface RpcRequest { id: number; op: RpcOp; args?: unknown }

/** Diagnostic class only: never used to grant authority or bypass publication fences. */
const rpcKind = (request: RpcRequest): string => {
  const singleton = request.op === "publish" || request.op === "publishReceipt";
  const batch = request.op === "publishBatch";
  const events = singleton ? [request.args] : batch && Array.isArray(request.args) ? request.args : [];
  if (events.length && events.every(event => isObject(event) && event.topic === "fabric.control.command")) return "control";
  if (events.length && events.every(event => isObject(event) && event.topic === "fabric.control.ack")) return "ack";
  return request.op === "mirror" || request.op === "presence" ? request.op : events.length ? "publish" : "read";
};

const lines = (input: Readable, onLine: (line: string) => void, onEnd: (error?: Error) => void): void => {
  let buffered = "";
  input.setEncoding("utf8");
  input.on("data", (chunk: string) => {
    buffered += chunk;
    let newline: number;
    while ((newline = buffered.indexOf("\n")) >= 0) {
      const line = buffered.slice(0, newline);
      buffered = buffered.slice(newline + 1);
      if (line.length > MAX_RPC_LINE_BYTES) {
        // A complete line over the limit fails the transport too.
        input.destroy();
        onEnd(new Error("Bridge transport line is too long"));
        return;
      }
      if (line.trim()) onLine(line);
    }
    if (buffered.length > MAX_RPC_LINE_BYTES) {
      input.destroy();
      onEnd(new Error("Bridge transport line is too long"));
    }
  });
  input.on("end", () => onEnd());
  input.on("close", () => onEnd());
  input.on("error", (error) => onEnd(error));
};

/**
 * The remote end of the transport (`mesh-bridge agent`), run by the forced ssh command. It serves
 * only the side operations above against its own mesh, and applies the same allow-list and the
 * pinned peer name to everything it is asked to write. Resolves when the input ends.
 */
export const serveBridgeAgent = (side: StoreBridgeSide, input: Readable, output: Writable,
  debug?: (message: string) => void): Promise<void> =>
  new Promise((resolve) => {
    // As with run()'s independent directions (#9751), serialize only related work.
    // hello fences later topology snapshots; all reads/publications retain one FIFO.
    const queues = { topology: Promise.resolve(), traffic: Promise.resolve() };
    const pending = debug ? { topology: 0, traffic: 0 } : undefined;
    let closed = false;
    let unsubscribe: (() => void) | undefined;
    const dispose = (): void => {
      const close = unsubscribe;
      unsubscribe = undefined;
      close?.();
    };
    const finish = (): void => {
      const wasClosed = closed;
      closed = true;
      dispose(); // also dispose a subscription returned after a reentrant finish()
      if (wasClosed) return;
      void Promise.allSettled([queues.topology, queues.traffic]).then(() => { dispose(); resolve(); });
    };
    const outputClosed = (error?: unknown): void => {
      if (error !== undefined) {
        const code = (error as NodeJS.ErrnoException).code;
        if (code !== "EPIPE" && code !== "ERR_STREAM_DESTROYED") throw error;
      }
      if (closed) return;
      finish();
      input.destroy();
    };
    output.on("error", outputClosed);
    output.on("close", () => outputClosed());
    output.on("finish", () => outputClosed());
    if (output.writableEnded || output.destroyed) outputClosed(output.errored ?? undefined);
    const reply = (value: unknown): void => {
      if (closed || output.writableEnded || output.destroyed) return;
      try { output.write(`${JSON.stringify(value)}\n`); }
      catch (error) { outputClosed(error); }
    };
    lines(
      input,
      (line) => {
        const receivedAt = Date.now(); // Receiver clock BEFORE either lane's dispatch/lock wait.
        const queuedAt = debug ? performance.now() : 0;
        let request: RpcRequest;
        try {
          request = JSON.parse(line) as RpcRequest;
        } catch {
          reply({ id: null, ok: false, error: "Malformed bridge request" });
          return;
        }
        const lane = request?.op === "hello" || request?.op === "presence" || request?.op === "mirror" ? "topology" : "traffic";
        // Requests ahead in THIS lane, including its in-service request; unrelated work
        // must not inflate a traffic queue depth while a topology service is held.
        const queueDepth = pending ? pending[lane]++ : 0;
        queues[lane] = queues[lane].then(async () => {
          if (closed) { if (pending) pending[lane]--; return; } // stdin ended while queued
          const startedAt = debug ? performance.now() : 0;
          let result: unknown;
          try {
            // Attach stdin listeners immediately, but prepare before the first hello/snapshot/write.
            // The store refuses rather than waits if a library caller already owns a bounded scope.
            await side.store.prepareLock();
            if (closed) { finish(); return; }
            if (request.op === "hello" && isObject(request.args) && request.args.changes === true && !unsubscribe) {
              // Unsolicited frames, never a hanging wait RPC ahead of publish.
              // This topology FIFO installs before hello's response and later snapshots.
              unsubscribe = side.subscribeChanges(
                (kind) => reply({ notification: "change", kind }),
                (error) => { reply({ notification: "failure", error: error.message }); input.destroy(error); finish(); },
              );
              if (closed) { finish(); return; }
            }
            result = await dispatch(side, request, receivedAt);
            if (closed) { finish(); return; }
            reply({ id: request.id, ok: true, result });
          } catch (error) {
            if (isIdempotencyKeyReused(error)) {
              // Exact code only; never forward the store's key, causes or payload.
              reply({ id: request.id, ok: false, code: FABRIC_IDEMPOTENCY_KEY_REUSED,
                error: "Fabric idempotency key reused with different input" });
              return;
            }
            reply({
              id: request.id, ok: false, error: error instanceof Error ? error.message : String(error),
              ...(error instanceof BridgeStaleIncarnationError ? { code: error.code,
                ...(safeDiagnosticId(error.rootId) && isCanonicalRootId(error.rootId) ? { rootId: error.rootId } : {}),
                ...(safeDiagnosticId(error.hostId) ? { hostId: error.hostId } : {}) }
                : isMeshIncarnationPending(error) ? { code: MESH_INCARNATION_PENDING_CODE }
                : isMeshSnapshotRetriesExhausted(error) ? { code: MESH_SNAPSHOT_RETRIES_EXHAUSTED_CODE }
                  : isMeshRetryableBusy(error) ? { code: MESH_LOCK_TIMEOUT_CODE } : {}),
            });
          } finally {
            if (debug) {
              const endedAt = performance.now();
              pending![lane]--;
              const presence = request.op === "mirror" ? request.args : request.op === "presence" ? result : undefined;
              debug(`bridge rpc ${JSON.stringify({ id: request.id, op: request.op, kind: rpcKind(request), queueDepth,
                queuedAt, startedAt, endedAt, queueMs: startedAt - queuedAt, runMs: endedAt - startedAt,
                requestBytes: Buffer.byteLength(line, "utf8"),
                ...(isObject(presence) && Array.isArray(presence.hosts) && Array.isArray(presence.participants)
                  ? { hosts: presence.hosts.length, participants: presence.participants.length,
                    presenceBytes: Buffer.byteLength(JSON.stringify(presence), "utf8") } : {}) })}`);
            }
          }
        });
      },
      finish,
    );
  });

// Host IDs need not equal root IDs in sibling diagnostics. Bound printable wire metadata;
// never interpolate an unchecked peer field (or the peer's arbitrary error text).
const safeDiagnosticId = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0 && value.length <= 256 && /^[\x20-\x7e]+$/.test(value);

const numberArg = (args: unknown, name: string): number => {
  const value = isObject(args) ? args[name] : undefined;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) throw new Error(`Bridge argument ${name} is invalid`);
  return value;
};

const presenceArg = (args: unknown, hub: string): BridgeMirrorPresence => {
  if (!isObject(args) || !Array.isArray(args.hosts) || !Array.isArray(args.participants)) {
    throw new Error("Bridge presence is invalid");
  }
  const hosts = args.hosts.flatMap((host): BridgeHost[] => {
    if (!isObject(host) || !isObject(host.record)) return [];
    const record = presenceHostOf(keyFor(HOST_PREFIX, String(host.record.id)), host.record, hub);
    return record && (remoteHostOf(record) === undefined || (record.bridgeVia === hub && record.remoteHost !== hub))
      ? [{ record, expiresAt: typeof host.expiresAt === "number" ? host.expiresAt : NaN,
        ...(host.observedAt !== undefined ? { observedAt: typeof host.observedAt === "number" ? host.observedAt : NaN } : {}) }] : [];
  });
  const participants = args.participants.flatMap((value): FabricParticipantRecord[] => {
    const record = isObject(value) ? participantOf(keyFor(PARTICIPANT_PREFIX, String(value.id)), value, hub) : undefined;
    return record && (remoteHostOf(record) === undefined || (record.bridgeVia === hub && record.remoteHost !== hub)) ? [record] : [];
  });
  const rawDropped = Array.isArray(args.dropped) ? args.dropped : [];
  const dropped = uniqueDroppedPresence(rawDropped);
  const owners = new Map(hosts.map(({ record }) => [record.id, record]));
  const malformedHosts = hosts.filter(({ record, expiresAt, observedAt }) => {
    const relayed = record.bridgeVia === hub && record.remoteHost !== undefined && record.remoteHost !== hub;
    const nonRoot = isNativeNonRootHost(record);
    return !isAdmissibleRecord("host", record, hub) || !Number.isFinite(expiresAt) ||
      (observedAt !== undefined && !Number.isFinite(observedAt)) || (!nonRoot && (record.identity.id !== record.id ||
        record.rootId !== record.id || (!relayed && record.bridgeVia !== undefined) || !CANONICAL_ID.test(record.id)));
  }).length;
  const malformedParticipants = participants.filter(record => {
    const owner = owners.get(record.ownerHostId);
    const relayed = record.bridgeVia === hub && record.remoteHost !== undefined && record.remoteHost !== hub;
    return !isAdmissibleRecord("participant", record, hub) || record.kind !== "root" || (!relayed && record.bridgeVia !== undefined) || !owner ||
      record.id !== owner.id || owner.identity.id !== record.ownerIdentityId || owner.rootId !== record.rootId;
  }).length;
  const inheritedCount = Math.max(validDroppedCount(args.droppedCount), relevantDroppedCount(rawDropped));
  const droppedCount = inheritedCount + malformedHosts + malformedParticipants +
    (args.hosts.length - hosts.length) + (args.participants.length - participants.length);
  const partial = args.partial === true || droppedCount > 0;
  return { hosts, participants, ...(dropped.length > 0 ? { dropped } : {}),
    ...(partial ? { partial: true } : {}), ...(droppedCount > 0 ? { droppedCount } : {}) };
};

const dispatch = async (side: StoreBridgeSide, request: RpcRequest, receivedAt: number): Promise<unknown> => {
  switch (request.op) {
    // Unversioned v1 clients must still accept a new agent during host-by-host rollout.
    // New clients request v2 explicitly; only a reported v1 peer can lack notifications.
    case "hello": return {
      version: isObject(request.args) && request.args.version === BRIDGE_PROTOCOL_VERSION
        ? BRIDGE_PROTOCOL_VERSION : LEGACY_BRIDGE_PROTOCOL_VERSION,
      tail: true, publishBatch: typeof side.publishBatch === "function", publishReceipts: true, changes: true, independentRpc: true,
      partialPresence: true, nonRootHosts: true,
    };
    case "latestSequence": return side.latestSequence();
    case "latestCursor": return side.latestCursor();
    case "tail": return side.tail(numberArg(request.args, "after"),
      isObject(request.args) && request.args.offset !== undefined ? numberArg(request.args, "offset") : undefined);
    case "read": return side.read(numberArg(request.args, "after"));
    case "presence": return { ...boundPresence(await side.presence()).presence, reserved: [] };
    case "publish": return side.publish(checkBridgePublish(request.args), [], true);
    case "publishReceipt": return side.publishWithReceipt(checkBridgePublish(request.args), [], true);
    case "publishBatch": {
      if (!Array.isArray(request.args) || !request.args.length || request.args.length > 256) throw new Error("Invalid bridge publish batch");
      // Remote wire never accepts held IDs: those are hub-local ownership fences.
      return side.publishBatch(request.args.map(event => ({ event: checkBridgePublish(event) })), true);
    }
    case "publishReceiptTimes": {
      if (!Array.isArray(request.args)) throw new Error("Invalid bridge publish receipt sequences");
      return side.publishReceipts(request.args as number[]);
    }
    case "mirror": return side.mirror(presenceArg(request.args, side.peer), receivedAt, true);
    case "bridgedIds": return side.bridgedIds(numberArg(request.args, "after"));
    default: throw new Error(`Unknown bridge operation: ${String(request.op)}`);
  }
};

class BridgeLegacyNotificationsError extends Error {}

/** One mirror stays in flight after its deadline; retries must not queue more RPCs behind it. */
export class BridgeMirrorDeferredError extends Error {
  readonly code: string;
  constructor(readonly timedOut: boolean, timeoutMs: number) {
    super(timedOut ? `Fabric bridge-stale: Bridge agent did not answer mirror within ${timeoutMs} ms; retrying with backoff` :
      "Bridge mirror is still in flight; retrying with backoff");
    this.name = "BridgeMirrorDeferredError";
    this.code = timedOut ? "FABRIC_BRIDGE_STALE" : "FABRIC_BRIDGE_MIRROR_PENDING";
  }
}
const isBridgeRetryable = (error: unknown): boolean => error instanceof BridgeMirrorDeferredError || isMeshRetryableBusy(error);

/** The hub's view of the remote side, over the transport's stdio. */
export class RemoteBridgeSide implements BridgeSide {
  #next = 1;
  #supportsTail = false;
  #supportsPartialPresence = false;
  #supportsNonRootHosts = false;
  #legacyCompletedPartials = 0;
  #legacyNoBaselineCycles = 0;
  #legacyNoBaselineSince: number | undefined;
  #legacyNoBaselineSinceMonotonic: number | undefined;
  #legacyMirrorSnapshot: LegacyMirrorSnapshot | undefined;
  #lastPresenceRenewalAt = Number.NEGATIVE_INFINITY;
  #independentRpc = false;
  #protocolVersion: number | undefined;
  #changesReady: Promise<void> | undefined;
  readonly #subscribers = new Set<{ changed: (kind: BridgeChange) => void; failed: (error: Error) => void }>();
  publishBatch?: NonNullable<BridgeSide["publishBatch"]>;
  publishWithReceipt?: NonNullable<BridgeSide["publishWithReceipt"]>;
  publishBatchWithReceipt?: NonNullable<BridgeSide["publishBatchWithReceipt"]>;
  #closed: Error | undefined;
  #mirrorPending: { id: number; error: BridgeMirrorDeferredError } | undefined;
  readonly #pending = new Map<number, { resolve: (value: unknown) => void; reject: (error: Error) => void }>();
  readonly #onClosed: (error: Error) => void;
  readonly closed: Promise<Error>;

  /**
   * Every call has a deadline. A slow mirror degrades this host without closing the transport;
   * exactly one mirror remains in flight until its late answer or close(). Other RPC deadlines
   * remain fatal. stop() still bounds its waits and closes the transport (security review F3).
   */
  constructor(readonly input: Readable, readonly output: Writable, readonly callTimeoutMs = DEFAULT_CALL_TIMEOUT_MS) {
    let onClosed!: (error: Error) => void;
    this.closed = new Promise((resolve) => (onClosed = resolve));
    this.#onClosed = onClosed;
    lines(
      input,
      (line) => {
        let response: { id?: unknown; ok?: unknown; result?: unknown; error?: unknown; code?: unknown; notification?: unknown; kind?: unknown;
          rootId?: unknown; hostId?: unknown };
        try {
          response = JSON.parse(line) as typeof response;
        } catch {
          return;
        }
        if (response.notification === "failure") {
          this.close(new Error(`Bridge agent notifications failed: ${String(response.error)}`));
          return;
        }
        if (response.notification === "change") {
          if (response.kind !== "events" && response.kind !== "presence" && response.kind !== "lease") {
            this.close(new Error("Invalid bridge change notification"));
            return;
          }
          for (const subscriber of this.#subscribers) subscriber.changed(response.kind);
          return;
        }
        const pending = typeof response.id === "number" ? this.#pending.get(response.id) : undefined;
        if (!pending) return;
        this.#pending.delete(response.id as number);
        const staleError = response.code === FABRIC_BRIDGE_STALE_INCARNATION ? new BridgeStaleIncarnationError(
          safeDiagnosticId(response.rootId) && isCanonicalRootId(response.rootId) ? response.rootId : undefined,
          safeDiagnosticId(response.hostId) ? response.hostId : undefined) : undefined;
        const mirrorPending = this.#mirrorPending;
        if (mirrorPending && mirrorPending.id === response.id) {
          const late = mirrorPending.error.timedOut;
          this.#mirrorPending = undefined;
          if (late && response.ok !== true && response.code !== MESH_LOCK_TIMEOUT_CODE &&
            response.code !== MESH_INCARNATION_PENDING_CODE && response.code !== MESH_SNAPSHOT_RETRIES_EXHAUSTED_CODE) {
            this.close(staleError ?? new Error(`Bridge agent: ${String(response.error)}`));
            return;
          }
          if (late) for (const subscriber of this.#subscribers) subscriber.changed("presence");
        }
        if (response.ok === true) pending.resolve(response.result);
        else {
          if (response.code === FABRIC_IDEMPOTENCY_KEY_REUSED) {
            pending.reject(new FabricIdempotencyKeyReusedError());
            return;
          }
          if (staleError) {
            pending.reject(staleError);
            return;
          }
          const error = new Error(`Bridge agent: ${String(response.error)}`);
          // Only allow-listed store refusal codes, never arbitrary peer error properties.
          if (response.code === MESH_LOCK_TIMEOUT_CODE || response.code === MESH_INCARNATION_PENDING_CODE ||
            response.code === MESH_SNAPSHOT_RETRIES_EXHAUSTED_CODE) Object.assign(error, { code: response.code });
          pending.reject(error);
        }
      },
      (error) => this.close(error ?? new Error("Bridge transport closed")),
    );
  }

  /** Fail pending calls, end remote stdin, and keep stdout readable until the child exits. */
  closeGracefully(error: Error = new Error("Bridge stopped")): void {
    this.#close(error, false);
  }

  /** Fail every pending and later call, and end both transport streams. */
  close(error: Error = new Error("Bridge transport closed")): void {
    this.#close(error, true);
  }

  #close(error: Error, destroyInput: boolean): void {
    if (this.#closed) return;
    this.#closed = error;
    for (const pending of this.#pending.values()) pending.reject(error);
    this.#pending.clear();
    this.#mirrorPending = undefined;
    this.#onClosed(error);
    for (const subscriber of this.#subscribers) subscriber.failed(error);
    this.#subscribers.clear();
    this.output.end();
    if (destroyInput) this.input.destroy();
  }

  #call<T>(op: RpcOp, args?: unknown, options: { timeoutMs?: number; fatalTimeout?: boolean } = {}): Promise<T> {
    if (this.#closed) return Promise.reject(this.#closed);
    // Legacy agents have one global FIFO. Negotiated agents can serve traffic while
    // mirroring, but never enqueue more topology work behind the one pending mirror.
    if (this.#mirrorPending && (!this.#independentRpc || op === "hello" || op === "presence" || op === "mirror")) {
      return Promise.reject(this.#mirrorPending.error);
    }
    const id = this.#next++;
    const timeoutMs = options.timeoutMs ?? this.callTimeoutMs;
    return new Promise<T>((resolve, reject) => {
      if (op === "mirror") this.#mirrorPending = { id, error: new BridgeMirrorDeferredError(false, timeoutMs) };
      let timeoutCheck: ReturnType<typeof setImmediate> | undefined;
      const timer = setTimeout(() => {
        if (op === "mirror") {
          const error = new BridgeMirrorDeferredError(true, timeoutMs);
          this.#mirrorPending = { id, error };
          // Keep this one response slot: its late ACK releases the gate, but never renews a
          // hub observation. The loop retries a NEW presence snapshot after backoff.
          reject(error);
          return;
        }
        const error = new Error(`Bridge agent did not answer ${op} within ${timeoutMs} ms`);
        if (options.fatalTimeout === false) {
          // After a delayed timer phase, let already-readable replies reach the I/O phase
          // before declaring a missed heartbeat. A silent peer still times out this turn.
          timeoutCheck = setImmediate(() => {
            const pending = this.#pending.get(id);
            if (pending) {
              this.#pending.delete(id);
              pending.reject(error);
            }
          });
          return;
        }
        this.close(error);
      }, timeoutMs);
      const settle = <V>(done: (value: V) => void) => (value: V): void => {
        clearTimeout(timer);
        if (timeoutCheck !== undefined) clearImmediate(timeoutCheck);
        done(value);
      };
      this.#pending.set(id, { resolve: settle(resolve as (value: unknown) => void), reject: settle(reject) });
      this.output.write(`${JSON.stringify({ id, op, ...(args === undefined ? {} : { args }) })}\n`);
    });
  }

  get protocolVersion(): number | undefined { return this.#protocolVersion; }
  /** Cumulative partial cycles completed as full snapshots for this legacy peer. */
  get legacyCompletedPartials(): number { return this.#legacyCompletedPartials; }
  /** Consecutive partial cycles skipped because no legacy complete baseline exists. */
  get legacyNoBaselineCycles(): number { return this.#legacyNoBaselineCycles; }
  get legacyNoBaselineSince(): number | undefined { return this.#legacyNoBaselineSince; }
  get legacyNoBaselineSinceMonotonic(): number | undefined { return this.#legacyNoBaselineSinceMonotonic; }
  /** Negotiated peer capabilities; undefined until hello completes. */
  get supportsPartialPresence(): boolean | undefined {
    return this.#protocolVersion === undefined ? undefined : this.#supportsPartialPresence;
  }
  get supportsNonRootHosts(): boolean | undefined {
    return this.#protocolVersion === undefined ? undefined : this.#supportsNonRootHosts;
  }
  /** Local time of the last acknowledged outbound presence mirror. */
  get lastPresenceRenewalAt(): number { return this.#lastPresenceRenewalAt; }
  get pendingMirrorTraffic(): boolean {
    return this.#independentRpc && this.#mirrorPending !== undefined && this.#closed === undefined;
  }

  async hello(changes = false, presenceOnly = false): Promise<void> {
    const reply = await this.#call<{ version?: unknown; tail?: unknown; publishBatch?: unknown; publishReceipts?: unknown; changes?: unknown;
      independentRpc?: unknown; partialPresence?: unknown; nonRootHosts?: unknown }>("hello", {
      version: BRIDGE_PROTOCOL_VERSION, ...(changes ? { changes: true } : {}),
    });
    if (reply?.version !== BRIDGE_PROTOCOL_VERSION && reply?.version !== LEGACY_BRIDGE_PROTOCOL_VERSION) {
      throw new Error(`Bridge agent speaks unsupported protocol ${String(reply?.version)}`);
    }
    if (this.#protocolVersion !== undefined && this.#protocolVersion !== reply.version) {
      throw new Error(`Bridge agent changed protocol ${this.#protocolVersion} to ${reply.version}; refusing protocol downgrade`);
    }
    this.#protocolVersion = reply.version;
    const supportsPartialPresence = reply.partialPresence === true;
    this.#supportsPartialPresence = supportsPartialPresence;
    this.#supportsNonRootHosts = reply.nonRootHosts === true;
    // Both ordinary hello and automatic notification setup negotiate dispatch isolation.
    this.#independentRpc = reply.independentRpc === true;
    // Notification setup must not silently switch a caller's chosen read/publication mode.
    // The CLI explicitly negotiates tail/batch; manual legacy users may have disabled them.
    if (!changes && !presenceOnly) {
      this.#supportsTail = reply.tail === true;
      if (reply.publishReceipts === true) {
        this.publishWithReceipt = event => this.#call<BridgePublishReceipt>("publishReceipt", event);
        if (reply.publishBatch === true) {
          this.publishBatchWithReceipt = async events => {
            const published = await this.publishBatch!(events);
            return published.length ? this.#call<BridgePublishReceipt[]>("publishReceiptTimes", published.map(({ sequence }) => sequence)) : [];
          };
        }
      }
      if (reply.publishBatch === true) this.publishBatch = events => this.#call<Array<{ sequence: number }>>("publishBatch", events.map(({ event }) => event));
    }
    if (reply.changes !== true) {
      if (reply.version === BRIDGE_PROTOCOL_VERSION) {
        throw new Error(`Bridge protocol ${reply.version} requires changes:true; refusing notification downgrade`);
      }
      if (changes) throw new BridgeLegacyNotificationsError(`Legacy bridge protocol ${reply.version} predates change notifications`);
    }
  }

  async subscribeChanges(changed: (kind: BridgeChange) => void, failed: (error: Error) => void): Promise<(() => void) | undefined> {
    if (this.#closed) throw this.#closed;
    const subscriber = { changed, failed };
    this.#subscribers.add(subscriber);
    try {
      await (this.#changesReady ??= this.hello(true));
      if (this.#closed) throw this.#closed;
      return () => { this.#subscribers.delete(subscriber); };
    } catch (error) {
      this.#subscribers.delete(subscriber);
      this.#changesReady = undefined;
      if (error instanceof BridgeLegacyNotificationsError) return undefined;
      throw error;
    }
  }

  latestSequence(): Promise<number> { return this.#call("latestSequence"); }
  watchdogHead(timeoutMs: number): Promise<number> {
    return this.#call("latestSequence", undefined, { timeoutMs, fatalTimeout: false });
  }
  read(after: number): Promise<BridgeRead> { return this.#call("read", { after }); }
  latestCursor(): Promise<BridgeHead> {
    return this.#supportsTail ? this.#call("latestCursor") : this.latestSequence().then(through => ({ through }));
  }
  tail(after: number, offset?: number): Promise<BridgeRead> {
    return this.#supportsTail ? this.#call("tail", { after, ...(offset !== undefined ? { offset } : {}) }) : this.read(after);
  }
  presence(): Promise<BridgePresence> { return this.#call("presence"); }
  publish(event: BridgePublish): Promise<{ sequence: number }> { return this.#call("publish", event); }
  async mirror(presence: BridgeMirrorPresence): Promise<void> {
    if (this.#closed) throw this.#closed;
    const partial = presenceIsPartial(presence);
    if (this.#protocolVersion === undefined) await this.hello(false, true);
    // Older receivers merge every same-root host token into one supersession history. Until a
    // peer explicitly advertises the isolated child-host admission path, send its canonical-root
    // projection only; never infer this capability from the protocol version alone.
    const outbound = this.#supportsNonRootHosts ? presence : legacyRootProjection(presence);
    if (partial && !this.#supportsPartialPresence) {
      // A legacy mirror treats every omission as withdrawal. Without an acknowledged complete
      // baseline, fail closed; otherwise hold each omitted root for at most three lease periods.
      if (!this.#legacyMirrorSnapshot) {
        this.#legacyNoBaselineCycles += 1;
        if (this.#legacyNoBaselineSince === undefined) {
          this.#legacyNoBaselineSince = Date.now();
          this.#legacyNoBaselineSinceMonotonic = monotonicClock.now();
        }
        return;
      }
      let complete = completeLegacyPartial(outbound, this.#legacyMirrorSnapshot, Date.now());
      if (Buffer.byteLength(JSON.stringify(complete.presence), "utf8") > BRIDGE_PAGE_BYTES) {
        complete = completeLegacyPartial({ hosts: [], participants: [] }, this.#legacyMirrorSnapshot, Date.now());
      }
      await this.#call("mirror", complete.presence);
      this.#legacyCompletedPartials += 1;
      this.#legacyMirrorSnapshot = complete;
      this.#lastPresenceRenewalAt = Date.now();
      return;
    }
    await this.#call("mirror", outbound);
    this.#lastPresenceRenewalAt = Date.now();
    if (!partial) {
      this.#legacyMirrorSnapshot = copyLegacySnapshot(outbound);
      this.#legacyNoBaselineCycles = 0;
      this.#legacyNoBaselineSince = undefined;
      this.#legacyNoBaselineSinceMonotonic = undefined;
    }
  }
  bridgedIds(after: number): Promise<BridgedIds> { return this.#call("bridgedIds", { after }); }
}

// ---------------------------------------------------------------------------------------------
// The bridge loop.

interface RelayGraceCursor {
  /** The retained event's source sequence; this is local cursor state, not peer authority. */
  sequence: number;
  /** Local wall-clock start of the bounded grace window. */
  startedAt: number;
  /** Optional event binding for cursor files written by newer bridges. */
  id?: string;
}

interface DirectionCursor {
  /** The last source sequence handled. */
  after: number;
  /** Source byte offset, tagged with the live-log generation; old cursor files omit it. */
  offset?: number;
  /** The destination sequence of the last event bridged in (restart dedupe starts after it). */
  mark: number;
  /** Optional v1 cursor extension: one retained relay boundary's local grace start. */
  relayGrace?: RelayGraceCursor;
}

interface CursorFile {
  format: 1;
  local: string;
  remote: string;
  toRemote: DirectionCursor;
  toLocal: DirectionCursor;
}

export interface MeshBridgeOptions {
  /** This (hub) side's name, as the remote sees it. */
  localName: string;
  /** The remote side's name, as the hub sees it. */
  remoteName: string;
  local: BridgeSide;
  remote: BridgeSide;
  cursorPath: string;
  /** Delivery-only hub-link IPC. Missing/unavailable fails closed for cross-spoke commands. */
  relay?: (destinationRemote: string, event: BridgePublish) => Promise<void>;
  /** Transitional legacy-remote polling only; new/new run() never polls. */
  pollMs?: number;
  /** Manual step() cadence only; run() follows origin updates and source expiry. */
  presenceMs?: number;
  /** Bound on each wait in stop(); the remote cannot hold a stop longer. */
  stopMs?: number;
  /** Initial missed-notification safety delay (default 1 s), doubled up to 30 s. Never expires/drops the event. */
  membershipRetryMs?: number;
  /** First wait after a WAL-cap refusal (then doubling to 30 s). Default 1 s; tests lower it. */
  walCapRetryMinMs?: number;
  log?: (message: string) => void;
  /** Debug lines (each event not addressed across); off when absent. */
  debug?: (message: string) => void;
  /**
   * The per-link bound on remote events that cross only through the hub factory exception
   * (default DEFAULT_FACTORY_RATE). Over it, the inbound drain stops at that event with the cursor
   * before it, and resumes when its token is due: the event waits, it is never dropped.
   */
  factoryRate?: FactoryRate;
  /** The clock of the once-a-minute log lines and the factory rate bound (Date.now by default; tests inject it). */
  clock?: () => number;
  /** Bounded peer round-trip cadence; tests scale these values without adding runtime configuration. */
  watchdogIntervalMs?: number;
  watchdogProbeTimeoutMs?: number;
  watchdogStallAfterMs?: number;
  directionStallAfterMs?: number;
  /** The one systemd notification call site, invoked after a responsive probe and at least one healthy direction. */
  notifyWatchdog?: () => void;
  /** The fabric.alarm bridge-stall envelope; without a publisher the durable stderr/reason-file signal remains. */
  publishStallAlarm?: (alarm: BridgeStallAlarm) => Promise<void>;
  /** Per-direction delivery-age owner alarm/recovery; never changes watchdog or transport state. */
  publishDeliveryAgeAlarm?: (alarm: BridgeDeliveryAgeAlarm) => Promise<void>;
  /** Addressed owner alarm for a legacy link that lacks an acknowledged complete baseline. */
  publishLegacyBaselineAlarm?: (alarm: BridgeLegacyBaselineAlarm) => Promise<void>;
}

export interface BridgeStepResult {
  toRemote: number;
  toLocal: number;
  dropped: number;
}

// Where the hub routes to: the peer's canonical ids only. A label, session id or name is never
// an address across, since a hub recipient's session name is not in any record the bridge can
// reserve (security review round 2, F1).
const remoteAddresses = (presence: Pick<BridgePresence, "hosts" | "participants">): Set<string> => {
  const names = new Set<string>();
  const hosts = presence.hosts.filter(({ record }) => isRootAuthorityHost(record));
  const rejectedHostIds = new Set(presence.hosts.filter(({ record }) => !isRootAuthorityHost(record)).map(({ record }) => record.id));
  for (const { record } of hosts) names.add(record.id).add(record.identity.id);
  for (const participant of presence.participants) {
    if (!isRootAuthorityParticipant(participant) || rejectedHostIds.has(participant.ownerHostId)) continue;
    names.add(participant.id).add(participant.rootId);
  }
  return names;
};

// Where the hub accepts events from the peer: its own canonical roots, by id or unique alias.
const addresses = (presence: Pick<BridgePresence, "hosts" | "participants">): Set<string> => {
  // Malformed, non-root and noncanonical native samples may remain for preservation but never
  // become routing authority. Trusted adapters may still supply a valid root participant alone.
  const hosts = presence.hosts.filter(({ record }) => isRootAuthorityHost(record));
  const rejectedHostIds = new Set(presence.hosts.filter(({ record }) => !isRootAuthorityHost(record)).map(({ record }) => record.id));
  const participants = presence.participants.filter(participant => isRootAuthorityParticipant(participant) &&
    !rejectedHostIds.has(participant.ownerHostId));
  const names = remoteAddresses({ hosts, participants });
  for (const participant of participants) {
    for (const alias of [participant.label, participant.sessionId]) if (alias) names.add(alias);
  }
  return names;
};

/**
 * A root's canonical id (`session:<session id>`); its host id and identity id are the same. An
 * admitted remote id must have this form. A native root never accepts a name in this namespace
 * other than its own id (RootInbox, ROOT_ID_PREFIX), and every native participant's id and name
 * is reserved, so no hub recipient but the remote owner answers to an admitted id.
 */
const CANONICAL_ID = new RegExp(`^${ROOT_ID_PREFIX}[A-Za-z0-9-]{8,128}$`);

/** Whether `id` is a root's canonical id, the only form a bridge admits (and a handoff names). */
export const isCanonicalRootId = (id: string): boolean => CANONICAL_ID.test(id);

const NON_ROOT_HOST_ID = /^(?:resident:[0-9a-f]{24}|runtime:[A-Za-z0-9._-]{1,128})$/;
const isNonRootHostId = (id: string, rootId: string): boolean => NON_ROOT_HOST_ID.test(id) && id !== rootId && CANONICAL_ID.test(rootId);
const isNonRootHostRecord = (record: FabricHostRecord): boolean => isNonRootHostId(record.id, record.rootId) &&
  isAdmissibleRecord("host", record);
/** Valid native runtime/resident host rows are retained for forwarding, not root authority. */
const isNativeNonRootHost = (record: FabricHostRecord): boolean =>
  remoteHostOf(record) === undefined && record.bridgeVia === undefined && isNonRootHostRecord(record);
const NON_ROOT_HOST_PREFIX_LIKE = /^(?:resident|runtime):/i;
/** Fail closed for recipient/sender authority: only an exact canonical root host may route. */
const isRootAuthorityHost = (record: FabricHostRecord): boolean => {
  if (NON_ROOT_HOST_PREFIX_LIKE.test(record.id) || !isCanonicalRootId(record.id)) return false;
  return isAdmissibleRecord("host", record) && record.rootId === record.id && record.identity.id === record.id;
};
const isRootAuthorityParticipant = (participant: FabricParticipantRecord): boolean =>
  isAdmissibleRecord("participant", participant) && participant.kind === "root" &&
  isCanonicalRootId(participant.id) && participant.id === participant.rootId &&
  participant.ownerHostId === participant.id && participant.ownerIdentityId === participant.id;
const legacyRootProjection = (presence: BridgeMirrorPresence): BridgeMirrorPresence => ({
  ...presence,
  // Preserve malformed canonical rows so the receiver can mark its own snapshot partial and
  // protect existing owners; only omit non-root/noncanonical host ids old receivers cannot safely merge.
  hosts: presence.hosts.filter(({ record }) => isCanonicalRootId(record.id) && !NON_ROOT_HOST_PREFIX_LIKE.test(record.id)),
  participants: presence.participants,
});

/**
 * The remote's presence as this link admits it: hosts and roots none of whose ids or aliases
 * are reserved on the hub (natives, live or not, and other links' mirrors), and roots under an
 * admitted host. Mirroring, routing and sender checks all use this snapshot, never the peer's
 * raw claims (security review F1/F2).
 */
export const admitPresence = (
  claimed: BridgeMirrorPresence,
  reserved: ReadonlySet<string>,
  trustedBridgeVia?: string,
): BridgeMirrorPresence => {
  // Remote presence is raw JSON: a malformed array or item is a dropped record, never evidence
  // that a previously mirrored root was absent.
  const rawValue: unknown = claimed;
  const raw = isObject(rawValue) ? rawValue : {};
  const rawHosts = Array.isArray(raw.hosts) ? raw.hosts : [];
  const rawParticipants = Array.isArray(raw.participants) ? raw.participants : [];
  // v1 bridges Main roots only, whose host id, identity id and root id are one id (security
  // review round 4, F2): a host that splits them is refused, so one id names one owner.
  const rawDropped = Array.isArray(raw.dropped) ? raw.dropped : [];
  const dropped = uniqueDroppedPresence(rawDropped);
  let droppedCount = Math.max(validDroppedCount(raw.droppedCount), relevantDroppedCount(rawDropped)) +
    (Array.isArray(raw.hosts) ? 0 : 1) + (Array.isArray(raw.participants) ? 0 : 1);
  const hosts: BridgeHost[] = [];
  const nonRootHosts: BridgeHost[] = [];
  for (const value of rawHosts) {
    if (!isObject(value) || !isObject(value.record) || typeof value.record.id !== "string") {
      droppedCount += 1;
      continue;
    }
    const record = presenceHostOf(keyFor(HOST_PREFIX, value.record.id), value.record, trustedBridgeVia);
    if (!record) {
      droppedCount += 1;
      const attribution = droppedPresenceOf("host", value.record);
      if (attribution) dropped.push(attribution);
      continue;
    }
    const expiresAt = typeof value.expiresAt === "number" ? value.expiresAt : NaN;
    const hasObservedAt = Object.hasOwn(value, "observedAt");
    const observedAt = typeof value.observedAt === "number" ? value.observedAt : hasObservedAt ? NaN : undefined;
    const relayed = trustedRelayOf(record, trustedBridgeVia);
    const nonRoot = isNativeNonRootHost(record);
    const malformed = !isAdmissibleRecord("host", record, trustedBridgeVia) || !Number.isFinite(expiresAt) ||
      (observedAt !== undefined && !Number.isFinite(observedAt)) || (!nonRoot &&
        (record.identity.id !== record.id || record.rootId !== record.id || (!relayed && record.bridgeVia !== undefined) ||
          !CANONICAL_ID.test(record.id)));
    const reservedConflict = [record.id, record.identity.id, record.rootId].some(id => reserved.has(id));
    if (malformed || reservedConflict) {
      if (malformed) {
        droppedCount += 1;
        const attribution = droppedPresenceOf("host", record, !reservedConflict);
        if (attribution) dropped.push(attribution);
      }
    } else if (nonRoot) nonRootHosts.push({ record, expiresAt, ...(observedAt !== undefined ? { observedAt } : {}) });
    else hosts.push({ record, expiresAt, ...(observedAt !== undefined ? { observedAt } : {}) });
  }
  const admitted = new Map(hosts.map(({ record }) => [record.id, record]));
  const participants: FabricParticipantRecord[] = [];
  for (const value of rawParticipants) {
    if (!isObject(value) || typeof value.id !== "string") { droppedCount += 1; continue; }
    const participant = participantOf(keyFor(PARTICIPANT_PREFIX, value.id), value, trustedBridgeVia);
    if (!participant) { droppedCount += 1; continue; }
    const owner = admitted.get(participant.ownerHostId);
    const relayed = trustedRelayOf(participant, trustedBridgeVia);
    const canonicalParticipant = participant.kind === "root" && (relayed || participant.bridgeVia === undefined) &&
      participant.id === participant.ownerHostId && participant.id === participant.ownerIdentityId && participant.id === participant.rootId;
    const ownerMatches = owner !== undefined && owner.id === participant.id &&
      owner.identity.id === participant.ownerIdentityId && owner.rootId === participant.rootId;
    const reservedConflict = [participant.id, participant.rootId, participant.ownerIdentityId, participant.sessionId]
      .some((id) => typeof id === "string" && reserved.has(id));
    if (!canonicalParticipant || !ownerMatches || reservedConflict) {
      const malformed = !canonicalParticipant || (owner !== undefined && !ownerMatches) || (!reservedConflict && owner === undefined);
      if (malformed) {
        droppedCount += 1;
        const attribution = droppedPresenceOf("participant", participant, !reservedConflict);
        if (attribution) dropped.push(attribution);
      }
    } else participants.push(participant);
  }
  const unique = uniqueDroppedPresence(dropped, true);
  const partial = !isObject(rawValue) || raw.partial === true || droppedCount > 0;
  return { hosts: [...hosts, ...nonRootHosts], participants, ...(unique.length > 0 ? { dropped: unique } : {}),
    ...(partial ? { partial: true } : {}), ...(droppedCount > 0 ? { droppedCount } : {}) };
};

/**
 * A presence snapshot that fits one frame: whole hosts with their roots, in order, until the
 * budget. If any host does not fit, the snapshot is partial and cannot prove omission; the bridge
 * logs the dropped count.
 */
export const boundPresence = (
  presence: BridgeMirrorPresence,
  budget = BRIDGE_PAGE_BYTES,
): { presence: BridgeMirrorPresence; dropped: number } => {
  const rawValue: unknown = presence;
  const raw = isObject(rawValue) ? rawValue : {};
  const rawHosts = Array.isArray(raw.hosts) ? raw.hosts : [];
  const rawParticipants = Array.isArray(raw.participants) ? raw.participants : [];
  const rawDropped = Array.isArray(raw.dropped) ? raw.dropped : [];
  const roots = new Map<string, FabricParticipantRecord[]>();
  let malformed = (Array.isArray(raw.hosts) ? 0 : 1) + (Array.isArray(raw.participants) ? 0 : 1);
  for (const value of rawParticipants) {
    if (!isObject(value) || typeof value.ownerHostId !== "string") { malformed += 1; continue; }
    roots.set(value.ownerHostId, [...(roots.get(value.ownerHostId) ?? []), value as unknown as FabricParticipantRecord]);
  }
  const hosts: BridgeHost[] = [];
  const participants: FabricParticipantRecord[] = [];
  let bytes = 0;
  let dropped = 0;
  for (const value of rawHosts) {
    if (!isObject(value) || !isObject(value.record) || typeof value.record.id !== "string" ||
      typeof value.expiresAt !== "number") { malformed += 1; continue; }
    const host = value as unknown as BridgeHost;
    const own = roots.get(host.record.id) ?? [];
    const size = Buffer.byteLength(JSON.stringify([host, own]), "utf8");
    if (bytes + size > budget) {
      dropped += 1;
      continue;
    }
    bytes += size;
    hosts.push(host);
    participants.push(...own);
  }
  const droppedCount = Math.max(validDroppedCount(raw.droppedCount), relevantDroppedCount(rawDropped)) + malformed + dropped;
  const droppedPresence = uniqueDroppedPresence(rawDropped, true);
  return { presence: { hosts, participants, ...(droppedPresence.length > 0 ? { dropped: droppedPresence } : {}),
    ...(raw.partial === true || droppedCount > 0 ? { partial: true } : {}), ...(droppedCount > 0 ? { droppedCount } : {}) }, dropped };
};

/**
 * Each admitted sender id's identity, from this link's canonical record for it (pi-fabric#876
 * security pass, (b)). admitPresence makes a host's id, identity id and root id one id, and a root
 * participant's id that same id, so the host record's identity is the sender's. A bridged event's
 * `from` is this, never the name, kind or session id its body claims.
 */
const senderIdentities = (presence: Pick<BridgePresence, "hosts" | "participants">): Map<string, MeshIdentity> => {
  const identities = new Map<string, MeshIdentity>();
  for (const { record } of presence.hosts) {
    if (!isRootAuthorityHost(record)) continue;
    const { id, name, kind, sessionId } = record.identity;
    identities.set(id, { id, name, kind, ...(typeof sessionId === "string" ? { sessionId } : {}) });
  }
  return identities;
};

const sameIdentity = (a: MeshIdentity, b: MeshIdentity): boolean =>
  a.id === b.id && a.name === b.name && a.kind === b.kind &&
  (typeof a.sessionId === "string" ? a.sessionId : undefined) === (typeof b.sessionId === "string" ? b.sessionId : undefined);

/** What one direction's forwarding is checked against: the link's authority for that page. */
interface BridgeRules {
  /** Direct native addresses: hub-local natives inbound, the peer's records outbound. Only these may hold a full ACK at the hub. */
  recipients: Set<string>;
  /** Inbound only: other links' canonical addresses (remoteAddresses(relays)); routable through the hub, never hub-local. */
  relayRecipients?: Set<string>;
  senders?: Set<string>;
  /** Inbound only: each admitted sender's canonical identity (senderIdentities). */
  identities?: Map<string, MeshIdentity>;
  reserved?: Set<string>;
  /** Outbound only: ids proven native to the hub or reserved by another link, not this peer. */
  reservedRecipients?: Set<string>;
  /** Direct other-link origins, never supplied by an untrusted remote. */
  relayOrigins?: Map<string, string>;
}

const senders = (presence: Pick<BridgePresence, "hosts" | "participants">): Set<string> => {
  const ids = new Set<string>();
  for (const { record } of presence.hosts) if (isRootAuthorityHost(record)) ids.add(record.identity.id);
  const rejectedHostIds = new Set(presence.hosts.filter(({ record }) => !isRootAuthorityHost(record)).map(({ record }) => record.id));
  for (const participant of presence.participants) if (isRootAuthorityParticipant(participant) &&
    !rejectedHostIds.has(participant.ownerHostId)) ids.add(participant.id).add(participant.ownerIdentityId);
  return ids;
};

export class MeshBridge {
  #cursor: CursorFile | undefined;
  #seen = { toRemote: new Set<string>(), toLocal: new Set<string>() };
  #presenceAt = Number.NEGATIVE_INFINITY;
  #presenceRetryAt = Infinity;
  #presenceSync: Promise<void> | undefined;
  #staleAdmission: BridgeStaleIncarnationError | undefined;
  #stopped = false;
  #stopTask: Promise<void> | undefined;
  #ready = false;
  readonly #relayDeliveries = new Set<Promise<void>>();
  #wake: (() => void) | undefined;
  #notificationMode = false;
  #changesPending = false;
  #notificationError: Error | undefined;
  #presenceVersion = 0;
  #syncedPresenceVersion = 0;
  #leaseVersion = 0;
  #syncedLeaseVersion = 0;
  #leaseRenewAt = Infinity;
  #legacyPolling = false;
  #expiryAt = Infinity;
  /** The loop's current pass, so stop() can fence it before the final withdrawal. */
  #running: Promise<void> = Promise.resolve();
  /** run()'s two direction loops (smarty-dev#9751); stop() fences on them too. */
  #directions: Promise<void> = Promise.resolve();
  readonly #directionPending: Record<Direction, boolean> = { toRemote: false, toLocal: false };
  readonly #directionBackoff: Record<Direction, boolean> = { toRemote: false, toLocal: false };
  readonly #directionMirrorBackoff: Record<Direction, boolean> = { toRemote: false, toLocal: false };
  readonly #directionWake: Partial<Record<Direction, (reason?: DirectionWakeReason) => void>> = {};
  readonly #directionWakeReason: Partial<Record<Direction, DirectionWakeReason>> = {};
  /** The retained-boundary retry kind currently being consumed by a direction pass. */
  readonly #membershipAttempt: Partial<Record<Direction, "safety" | "grace">> = {};
  /** Next missed-notification delay; a real notification clears it, sync completion does not. */
  readonly #membershipSafetyDelay: Partial<Record<Direction, number>> = {};
  /** Last sampled, admitted source deadlines: evidence to WAIT only, never authority to publish. */
  #sampledDeadlines = new Map<string, number>();
  /** At most one pending boundary per direction. Bodies stay in the durable source, before its cursor. */
  readonly #membershipRetryAt: Partial<Record<Direction, number>> = {};
  readonly #membershipSequence: Partial<Record<Direction, number>> = {};
  /** Events not addressed across since the last count line (smarty-dev#9352). */
  #unaddressed = { count: 0, toRemote: 0, toLocal: 0, factory: 0, safety: 0, topics: new Set<string>(), more: false };
  #unaddressedLoggedAt = Number.NEGATIVE_INFINITY;
  #presenceInfoLoggedAt = Number.NEGATIVE_INFINITY;
  #legacyNoBaselineWarned = false;
  #legacyNoBaselineOverdueReported = false;
  // Owner-page episode and retry state is separate from the informational hold deadline.
  readonly #legacyBaselineAlarm = new BridgeLegacyBaselineAlarmEpisode();
  /** The factory exception's token buckets (pi-fabric#876 review, P1-2; security pass, (c)). */
  readonly #factoryRate: Required<FactoryRate>;
  #factoryTokens: number;
  /** Bytes left in the byte bucket; below zero after an event bigger than its burst (a debt). */
  #factoryBytes: number;
  #factoryRefilledAt = Number.NEGATIVE_INFINITY;
  /** A deferred factory event holds the inbound drain until this time (this bridge's clock). */
  #factoryWaiting = false;
  /** Command id to the hub sender's replyTo, for commands this link carried out (smarty-dev#9751). */
  readonly #awaitingAck = new Map<string, { replyTo: string; target: string; targetId: string; expiresAt: number }>();
  #factoryRetryAt = Number.NEGATIVE_INFINITY;
  #factoryDeferrals = 0;
  #factoryLoggedAt = Number.NEGATIVE_INFINITY;
  #lastCursorAfter: Partial<Record<Direction, number>> = {};
  #directionHeads: Partial<Record<Direction, number>> = {};
  #directionPendingSince: Partial<Record<Direction, number>> = {};
  #lastOkToRemote: number | undefined;
  #lastOkToLocal: number | undefined;
  #lastOkToRemoteMono: number | undefined;
  #lastOkToLocalMono: number | undefined;
  #lastRoundTripAt: number | undefined;
  #stallActive = false;
  #stallDirection: Direction | "both" | undefined;
  #watchdogTask: Promise<void> | undefined;
  #watchdogWake: (() => void) | undefined;
  readonly #deliveryAge: BridgeDeliveryAgeMonitor;

  constructor(readonly options: MeshBridgeOptions) {
    this.#deliveryAge = new BridgeDeliveryAgeMonitor({ host: options.localName, peer: options.remoteName,
      ...(options.clock ? { clock: options.clock } : {}),
      log: line => this.#log(line), ...(options.publishDeliveryAgeAlarm ? { alarm: options.publishDeliveryAgeAlarm } : {}) });
    for (const name of [options.localName, options.remoteName]) {
      if (!validBridgeName(name)) throw new Error(`Invalid bridge name: ${name}`);
    }
    if (options.localName === options.remoteName) throw new Error("Bridge side names must differ");
    if (options.membershipRetryMs !== undefined && !(Number.isSafeInteger(options.membershipRetryMs) && options.membershipRetryMs > 0)) {
      throw new Error("membershipRetryMs needs a positive integer");
    }
    for (const [name, value] of Object.entries({ watchdogIntervalMs: options.watchdogIntervalMs,
      watchdogProbeTimeoutMs: options.watchdogProbeTimeoutMs, watchdogStallAfterMs: options.watchdogStallAfterMs,
      directionStallAfterMs: options.directionStallAfterMs })) {
      if (value !== undefined && !(Number.isSafeInteger(value) && value > 0)) throw new Error(`${name} needs a positive integer`);
    }
    const given = options.factoryRate ?? DEFAULT_FACTORY_RATE;
    const rate: Required<FactoryRate> = {
      perMinute: given.perMinute, burst: given.burst,
      bytesPerMinute: given.bytesPerMinute ?? DEFAULT_FACTORY_RATE.bytesPerMinute,
      byteBurst: given.byteBurst ?? DEFAULT_FACTORY_RATE.byteBurst,
    };
    if (!(Number.isFinite(rate.perMinute) && rate.perMinute > 0) || !(Number.isSafeInteger(rate.burst) && rate.burst > 0)) {
      throw new Error("factoryRate needs a positive perMinute and a positive integer burst");
    }
    if (!(Number.isSafeInteger(rate.bytesPerMinute) && rate.bytesPerMinute > 0) || !(Number.isSafeInteger(rate.byteBurst) && rate.byteBurst > 0)) {
      throw new Error("factoryRate needs a positive integer bytesPerMinute and byteBurst");
    }
    this.#factoryRate = rate;
    this.#factoryTokens = rate.burst;
    this.#factoryBytes = rate.byteBurst;
  }

  #now(): number {
    return (this.options.clock ?? Date.now)();
  }

  /** The factory buckets' event tokens now, both buckets refilled for the time since the last look. */
  #factoryTokensNow(): number {
    const now = this.#now();
    if (now > this.#factoryRefilledAt) {
      const { perMinute, burst, bytesPerMinute, byteBurst } = this.#factoryRate;
      const elapsed = now - this.#factoryRefilledAt;
      this.#factoryTokens = Math.min(burst, this.#factoryTokens + elapsed * perMinute / 60_000);
      this.#factoryBytes = Math.min(byteBurst, this.#factoryBytes + elapsed * bytesPerMinute / 60_000);
      this.#factoryRefilledAt = now;
    }
    return this.#factoryTokens;
  }

  /**
   * Whether one more factory event of `size` bytes fits, after `events` and `bytes` already
   * spent in this batch. An event bigger than the byte burst needs a full bucket and leaves a debt,
   * so it waits but is never stuck, and the bytes over any window stay bounded.
   */
  #factoryAffords(events: number, bytes: number, size: number): boolean {
    return events + 1 <= this.#factoryTokensNow() && bytes + Math.min(size, this.#factoryRate.byteBurst) <= this.#factoryBytes;
  }

  /**
   * The event a bridged event becomes on the far side. Its `from` is, inbound, the sender's
   * identity from this link's presence record (pi-fabric#876 security pass, (b)): the name, kind
   * and session id in the remote event's body never pass through, on any topic or recipient.
   * Outbound it is the hub's own event's sender.
   */
  #outbound(event: MeshEvent, rules: BridgeRules, direction: "toRemote" | "toLocal"): BridgePublish {
    if (this.#staleAdmission) throw this.#staleAdmission;
    const data = isObject(event.data) ? event.data : {};
    // smarty-dev#9751: a hub sender that is no participant (a factory host) still gets the ACK of a command
    // this link carried out for it. Only its own authenticated address: replyTo must be the carried command's
    // sender, published natively on the hub (verification "mesh", never a bridged copy), so no command can
    // point an ACK, and its result, at anyone else (pi-fabric#928 review P1). The record is bound to the
    // command's target and expires; bounded, oldest first out.
    if (direction === "toRemote" && event.topic === "fabric.control.command" && typeof data.commandId === "string" &&
        data.replyTo === event.from.id && event.verification === "mesh" && UNICAST_REPLY_TO.test(data.replyTo) &&
        typeof data.targetId === "string" && typeof event.to === "string") {
      const now = this.#now();
      this.#awaitingAck.delete(data.commandId);
      this.#awaitingAck.set(data.commandId, { replyTo: event.from.id, target: event.to, targetId: data.targetId, expiresAt: now + AWAITING_ACK_MS });
      for (const [id, awaited] of this.#awaitingAck) {
        if (this.#awaitingAck.size <= MAX_AWAITING_ACKS && awaited.expiresAt > now) break;
        this.#awaitingAck.delete(id);
      }
    }
    let from = event.from;
    if (direction === "toLocal" && rules.identities) {
      const identity = rules.identities.get(event.from.id);
      if (!identity) throw new BridgeOwnershipError(`${event.from.id} is no longer bound to bridge link ${this.options.remoteName}`);
      if (!sameIdentity(identity, event.from)) {
        this.options.debug?.(`toLocal: sender identity from its presence record, not the event: ${event.topic} ${event.id} from ${event.from.id}`);
      }
      from = identity;
    }
    // Sanitize legacy hub-log relays too. A current destination bridge must not replay
    // old private ACK text/results that reached the hub before the first-hop restriction.
    const relayedAck = event.topic === "fabric.control.ack" &&
      (direction === "toLocal" ? !rules.recipients.has(event.to!) :
        event.verification === "bridge" && bridgeStampOf(event)?.from !== this.options.localName);
    return {
      topic: event.topic, kind: event.kind, from,
      // ponytail: keep the required-recipient wire schema; only an admitted inbound record needs a role address.
      to: direction === "toLocal" && event.to === undefined && isHubFactoryEvent(event) ? HUB_FACTORY_RECIPIENT : event.to!,
      ...(event.verification === "mesh" || event.verification === "bridge" ? { principal: event.principal } : {}),
      ...(event.text !== undefined && !relayedAck ? { text: event.text } : {}),
      // An ACK that crosses only through the relay (its address is no hub recipient) carries just the outcome
      // fields, never the command's result payload (pi-fabric#928 SEC P1).
      data: { ...(relayedAck
        ? Object.fromEntries(Object.entries(data).filter(([field]) => RELAYED_ACK_FIELDS.has(field))) : data),
      bridge: direction === "toRemote" && bridgeStampOf(event)
        ? { ...bridgeStampOf(event)!, via: this.options.localName }
        : { from: direction === "toRemote" ? this.options.localName : this.options.remoteName, id: event.id } },
    } as BridgePublish;
  }

  /** Whether a remote event crosses only through the factory exception, so it spends a token. */
  #viaFactory(event: MeshEvent, rules: BridgeRules, direction: "toRemote" | "toLocal"): boolean {
    return direction === "toLocal" && !(typeof event.to === "string" && rules.recipients.has(event.to)) && isHubFactoryEvent(event);
  }

  /**
   * Over the factory rate: the inbound drain stops at this event, unforwarded, with the cursor
   * before it, and retries when its token is due (run() arms one timer for that; no notification is
   * needed and nothing polls). At most one line a minute reports the deferrals.
   */
  #deferFactory(event: MeshEvent, size: number): void {
    const now = this.#now();
    const { perMinute, burst, bytesPerMinute, byteBurst } = this.#factoryRate;
    const waitMs = Math.max(1, Math.ceil((1 - this.#factoryTokens) * 60_000 / perMinute),
      Math.ceil((Math.min(size, byteBurst) - this.#factoryBytes) * 60_000 / bytesPerMinute));
    this.#factoryWaiting = true;
    this.#factoryRetryAt = now + waitMs;
    this.#factoryDeferrals += 1;
    if (now - this.#factoryLoggedAt < FACTORY_RATE_LOG_MS) return;
    this.#factoryLoggedAt = now;
    this.#log(`toLocal: factory rate bound (${perMinute}/min, burst ${burst}; ${bytesPerMinute} bytes/min, burst ${byteBurst}): ${this.#factoryDeferrals} deferrals; ` +
      `waiting at ${event.topic} ${event.id}, retry in ${waitMs} ms; nothing dropped`);
    this.#factoryDeferrals = 0;
  }

  #log(message: string): void {
    this.options.log?.(message);
  }

  /** One per-link INFO line, no more than once a minute except the first baseline-outage alarm. */
  #reportPresenceInfo(): void {
    const now = this.#now();
    const remote = this.options.remote instanceof RemoteBridgeSide ? this.options.remote : undefined;
    const noBaselineCycles = remote?.legacyNoBaselineCycles ?? 0;
    const noBaselineSince = remote?.legacyNoBaselineSince;
    if (noBaselineCycles === 0) {
      this.#legacyNoBaselineWarned = false;
      this.#legacyNoBaselineOverdueReported = false;
    } else if (!this.#legacyNoBaselineWarned) {
      this.#log(`presence WARN: peer=${this.options.remoteName} partial cycles cannot be mirrored without a complete baseline; ` +
        `legacyNoBaselineCycles=${noBaselineCycles}; existing peer mirror is left untouched`);
      this.#legacyNoBaselineWarned = true;
    }
    if (remote && supportsLegacyBaselineAlarm(remote.supportsPartialPresence) && this.options.publishLegacyBaselineAlarm) {
      this.#legacyBaselineAlarm.observe({ peer: this.options.remoteName,
        sinceMs: noBaselineCycles > 0 ? noBaselineSince : undefined,
        sinceMonotonicMs: noBaselineCycles > 0 ? remote.legacyNoBaselineSinceMonotonic : undefined,
        at: now, monotonicAt: monotonicClock.now() }, this.options.publishLegacyBaselineAlarm,
      (error: unknown) => {
        this.#log(`presence alarm publish failed: peer=${this.options.remoteName}; ${error instanceof Error ? error.message : String(error)}`);
        this.#wake?.();
      });
    }
    const noBaselineOverdue = noBaselineCycles > 0 && noBaselineSince !== undefined &&
      now - noBaselineSince > LEGACY_PARTIAL_HOLD_MS;
    const overdueInfoDue = noBaselineOverdue && !this.#legacyNoBaselineOverdueReported;
    if (now - this.#presenceInfoLoggedAt < UNADDRESSED_LOG_MS && !overdueInfoDue) return;
    this.#presenceInfoLoggedAt = now;
    const renewedAt = remote?.lastPresenceRenewalAt ?? this.#presenceAt;
    const renewalAge = Number.isFinite(renewedAt) ? Math.max(0, now - renewedAt) : "never";
    this.#log(`presence INFO: peer=${this.options.remoteName} renewalAgeMs=${renewalAge} ` +
      `legacyCompletedPartials=${remote?.legacyCompletedPartials ?? 0} legacyNoBaselineCycles=${noBaselineCycles}` +
      (noBaselineOverdue ? " legacy presence not renewed (no complete baseline)" : ""));
    if (noBaselineOverdue) this.#legacyNoBaselineOverdueReported = true;
  }

  #logIdempotencyConflict(direction: Direction, event: MeshEvent, error: unknown): void {
    const data = isObject(event.data) ? event.data : {};
    const key = typeof data.commandId === "string" ? data.commandId
      : isObject(error) && typeof error.idempotencyKey === "string" ? error.idempotencyKey : "unknown";
    this.#log(`${direction}: dropped idempotency conflict sequence ${event.sequence} event ${event.id} key ${key}`);
  }

  /**
   * An event neither addressed to the peer nor admitted to it: one debug line each, and a count
   * for the INFO line (smarty-dev#9352: these drops were silent).
   */
  #notAddressed(direction: "toRemote" | "toLocal", event: MeshEvent): void {
    const unaddressed = this.#unaddressed;
    unaddressed.count += 1;
    unaddressed[direction] += 1;
    // A remote event to the factory's role that did not cross (pi-fabric#876 post-install gate).
    if (direction === "toLocal" && event.to === HUB_FACTORY_RECIPIENT) unaddressed.factory += 1;
    if (!unaddressed.topics.has(event.topic)) {
      if (unaddressed.topics.size < UNADDRESSED_TOPICS) unaddressed.topics.add(event.topic);
      else unaddressed.more = true;
    }
    this.options.debug?.(`${direction}: not addressed across: ${event.topic} ${event.id} from ${
      isObject(event.from) ? String(event.from.id) : "?"} to ${String(event.to ?? "(none)")}`);
    this.#reportUnaddressed();
  }

  /**
   * The INFO count line, at most once a minute. Event-driven: a drop or a forward after the
   * minute emits it; an idle bridge arms no timer for it.
   */
  #reportUnaddressed(): void {
    const unaddressed = this.#unaddressed;
    if (unaddressed.count === 0 && unaddressed.safety === 0) return;
    const now = this.#now();
    if (now - this.#unaddressedLoggedAt < UNADDRESSED_LOG_MS) return;
    this.#unaddressedLoggedAt = now;
    this.#unaddressed = { count: 0, toRemote: 0, toLocal: 0, factory: 0, safety: 0, topics: new Set<string>(), more: false };
    this.#log(`bridge: ${unaddressed.count} events not addressed across (topics: ${[...unaddressed.topics].join(", ")}${
      unaddressed.more ? ", ..." : ""}; toRemote ${unaddressed.toRemote}, toLocal ${unaddressed.toLocal} (to factory ${unaddressed.factory})${
        unaddressed.safety > 0 ? `; safety wakes advancing retained boundaries ${unaddressed.safety}` : ""})`);
  }

  async #reconcileSeen(): Promise<void> {
    const cursor = this.#cursor!; // start() initialized it, or run() already observed readiness.
    const [toRemote, toLocal] = await Promise.all([
      allBridgedIds(this.options.remote, cursor.toRemote.mark),
      allBridgedIds(this.options.local, cursor.toLocal.mark),
    ]);
    this.#seen = { toRemote, toLocal };
  }

  /** Restore event recovery and reconcile live participants on every (re)connect. */
  async start(): Promise<void> {
    if (this.#stopped) throw relayUnavailable();
    this.#ready = false;
    // Cursors recover events, not presence. A reconnect may follow lease expiry or pruning,
    // even when this instance's previous presence pass would not yet be due (#5036).
    this.#presenceAt = Number.NEGATIVE_INFINITY;
    const { local, remote, localName, remoteName } = this.options;
    let saved: CursorFile | undefined;
    try {
      const value = JSON.parse(readFileRetrying(this.options.cursorPath)) as CursorFile;
      if (value?.format === 1 && value.local === localName && value.remote === remoteName) saved = value;
      else this.#log(`cursor ${this.options.cursorPath} is for another bridge; starting at the heads`);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
    const retryDeadline = Date.now() + BRIDGE_STARTUP_RETRY_BUDGET_MS;
    let retryMs = LOCK_RETRY_MIN_MS;
    let cursorInitialized = false;
    for (;;) {
      if (this.#stopped) throw relayUnavailable();
      try {
        if (!cursorInitialized) {
          if (saved) {
            this.#cursor = saved;
            this.#lastCursorAfter = { toRemote: saved.toRemote.after, toLocal: saved.toLocal.after };
          } else {
            const [localHead, remoteHead]: [BridgeHead, BridgeHead] = await Promise.all([
              local.latestCursor?.() ?? local.latestSequence().then(through => ({ through })),
              remote.latestCursor?.() ?? remote.latestSequence().then(through => ({ through })),
            ]);
            this.#cursor = {
              format: 1, local: localName, remote: remoteName,
              toRemote: { after: localHead.through, ...(localHead.offset !== undefined ? { offset: localHead.offset } : {}), mark: remoteHead.through },
              toLocal: { after: remoteHead.through, ...(remoteHead.offset !== undefined ? { offset: remoteHead.offset } : {}), mark: localHead.through },
            };
            this.#save();
          }
          cursorInitialized = true;
        }
        await this.#reconcileSeen();
        // Read CURRENT natives (including participants/ files), never a saved projection set.
        // Reconcile against the destination under its lock: unchanged digests skip state writes,
        // but lost records must be recreated and missing sources must not have their lease renewed.
        await this.syncPresence();
        break;
      } catch (error) {
        const remaining = retryDeadline - Date.now();
        if (this.#stopped || !isBridgeRetryable(error) || remaining <= 0) throw error;
        const waitMs = Math.min(retryMs, remaining);
        this.#log(`bridge startup ${meshBusyLabel(error)}; retrying in ${waitMs} ms`);
        await new Promise<void>(resolve => setTimeout(resolve, waitMs));
        retryMs = Math.min(retryMs * 2, LOCK_RETRY_MAX_MS);
      }
    }
    if (!this.#stopped) {
      this.#ready = true;
      this.#lastRoundTripAt = monotonicClock.now();
      this.#clearStallReason();
    }
  }

  /**
   * Destination-link admission for authenticated delivery-only IPC. The sourceRemote is
   * supplied by the UDS registry, never inferred from the event. No hub publication here.
   */
  deliverRelay(sourceRemote: string, input: BridgePublish): Promise<void> {
    const work = this.#deliverRelay(sourceRemote, input).catch((error: unknown) => {
      if (isIdempotencyKeyReused(error)) throw new FabricIdempotencyKeyReusedError();
      throw relayUnavailable();
    });
    this.#relayDeliveries.add(work);
    void work.then(() => this.#relayDeliveries.delete(work), () => this.#relayDeliveries.delete(work));
    return work;
  }

  async #deliverRelay(sourceRemote: string, input: BridgePublish): Promise<void> {
    const { local, remote, localName, remoteName } = this.options;
    const ready = (): boolean => this.#ready && !this.#stopped && !this.#notificationError;
    if (!ready() || !validBridgeName(sourceRemote) || sourceRemote === remoteName || sourceRemote === localName) throw relayUnavailable();
    const pub = checkBridgePublish(input);
    const stamp = pub.data.bridge;
    if (pub.topic !== "fabric.control.command" || stamp.from !== sourceRemote || hasHubHop(pub) || !stamp.id ||
      pub.data.destinationRemoteHost !== remoteName || typeof pub.data.commandId !== "string" || !pub.data.commandId) throw relayUnavailable();
    // One normal refresh when due, then fresh hub authority; no remote-supplied origin maps.
    const { remote: owned } = await this.#authority();
    const relays = await local.relayPresence?.() ?? { hosts: [], participants: [] };
    const identities = senderIdentities(relays);
    const identity = identities.get(pub.from.id);
    const rules: BridgeRules = { recipients: remoteAddresses(owned),
      relayOrigins: new Map(relays.hosts.map(host => [host.record.id, host.record.remoteHost!])) };
    const event: MeshEvent = { ...pub, id: stamp.id, sequence: 0, createdAt: this.#now(), verification: "bridge" };
    if (!ready() || !identity || !sameIdentity(identity, pub.from) || this.#refusal(event, rules, "toRemote") ||
      local.relayOrigin?.(pub.from.id) !== sourceRemote || !local.holds?.(pub.to)) throw relayUnavailable();
    // Reuse this destination's OWN connected remote side, preserving the original source
    // event id. The destination store's commandId dedupe survives lost UDS receipts/retries.
    const receipt = await remote.publish({ ...pub, from: identity, data: { ...pub.data, bridge: { ...stamp, via: localName } } });
    // A fulfilled RPC is not confirmation by itself. Missing/malformed receipts are
    // delivery-unknown, just like a lost response, and cannot authorize a hub envelope.
    if (!Number.isSafeInteger(receipt?.sequence) || receipt.sequence <= 0) throw relayUnavailable();
  }

  #save(): void {
    fs.mkdirSync(path.dirname(this.options.cursorPath), { recursive: true, mode: 0o700 });
    writeJsonAtomic(this.options.cursorPath, this.#cursor, { mode: 0o600, durable: true });
    if (!this.#cursor) return;
    const now = Date.now();
    for (const direction of ["toRemote", "toLocal"] as const) {
      const after = this.#cursor[direction].after;
      const previous = this.#lastCursorAfter[direction];
      this.#lastCursorAfter[direction] = after;
      if (previous === undefined || after > previous) this.#noteDirectionSuccess(direction, now, monotonicClock.now());
    }
  }

  #noteDirectionSuccess(direction: Direction, at: number, monotonicAt: number): void {
    if (direction === "toRemote") {
      this.#lastOkToRemote = at;
      this.#lastOkToRemoteMono = monotonicAt;
    } else {
      this.#lastOkToLocal = at;
      this.#lastOkToLocalMono = monotonicAt;
    }
    // A still-pending backlog restarts at actual cursor progress, not the next probe.
    // Crossing its last sampled head ends that backlog: later work must not inherit idle time.
    const head = this.#directionHeads[direction];
    if (head !== undefined && this.#cursor && this.#cursor[direction].after < head) {
      this.#directionPendingSince[direction] = monotonicAt;
    } else delete this.#directionPendingSince[direction];
  }

  #clearStallReason(): void {
    try { writeFileAtomic(bridgeStallReasonPath(this.options.cursorPath), "", { mode: 0o600, durable: true }); }
    catch { this.#log("could not clear the previous bridge stall reason file"); }
  }

  #recordStallReason(line: string): void {
    this.#log(line.slice("mesh-bridge: ".length));
    try { writeFileAtomic(bridgeStallReasonPath(this.options.cursorPath), `${line}\n`, { mode: 0o600, durable: true }); }
    catch { this.#log("could not atomically write the bridge stall reason file"); }
  }

  #clearStallOnProgress(direction: Direction): void {
    if (!this.#stallActive || (this.#stallDirection !== "both" && this.#stallDirection !== direction)) return;
    this.#stallActive = false;
    this.#stallDirection = undefined;
    this.#clearStallReason();
  }

  async #raiseStall(direction: Direction | "both", reason: string, fatal: boolean): Promise<void> {
    if (this.#stopped) return;
    if (!this.#stallActive) {
      this.#stallActive = true;
      this.#stallDirection = direction;
      const atMs = Date.now();
      const at = new Date(atMs).toISOString();
      const safeReason = reason.replace(/[\r\n]/g, " ");
      const alarm: BridgeStallAlarm = {
        v: 1, host: this.options.localName, peer: this.options.remoteName, direction,
        lastOkToRemote: this.#lastOkToRemote === undefined ? null : new Date(this.#lastOkToRemote).toISOString(),
        lastOkToLocal: this.#lastOkToLocal === undefined ? null : new Date(this.#lastOkToLocal).toISOString(),
        stallId: nextBridgeStallId(), reason: safeReason, at,
      };
      this.#recordStallReason(`mesh-bridge: stall v1 host=${alarm.host} peer=${alarm.peer} direction=${alarm.direction} at=${alarm.at} ` +
        `lastOkToRemote=${alarm.lastOkToRemote ?? "never"} lastOkToLocal=${alarm.lastOkToLocal ?? "never"} ` +
        `stallId=${alarm.stallId} reason=${alarm.reason} restartCount=unknown`);
      try { await this.options.publishStallAlarm?.(alarm); }
      catch { this.#log("could not publish fabric.alarm bridge-stall"); }
    }
    if (fatal && !this.#stopped) {
      const error = new Error(`Bridge ${direction} stalled`);
      if (this.options.remote.closeGracefully) this.options.remote.closeGracefully(error);
      else this.options.remote.close?.(error);
    }
  }

  async #watchdogProbe(): Promise<void> {
    const cursor = this.#cursor;
    if (!cursor || this.#stopped) return;
    const timeoutMs = this.options.watchdogProbeTimeoutMs ?? BRIDGE_WATCHDOG_PROBE_TIMEOUT_MS;
    let direction: Direction | "both" = "both";
    try {
      const remoteHead = await (this.options.remote.watchdogHead?.(timeoutMs) ?? this.options.remote.latestSequence());
      const localHead = await this.options.local.latestSequence();
      if (this.#stopped) return;
      if (!Number.isSafeInteger(remoteHead) || remoteHead < 0 || !Number.isSafeInteger(localHead) || localHead < 0) {
        throw new Error("invalid mesh sequence from watchdog probe");
      }
      const wallNow = Date.now();
      const now = monotonicClock.now();
      this.#directionHeads = { toRemote: localHead, toLocal: remoteHead };
      const pendingToRemote = cursor.toRemote.after < localHead;
      const pendingToLocal = cursor.toLocal.after < remoteHead;
      // Start the deadline when pending work is observed, not at the last success:
      // time spent idle (or awaiting this probe) is not time spent stalled.
      if (!pendingToRemote) this.#noteDirectionSuccess("toRemote", wallNow, now);
      else this.#directionPendingSince.toRemote ??= now;
      if (!pendingToLocal) this.#noteDirectionSuccess("toLocal", wallNow, now);
      else this.#directionPendingSince.toLocal ??= now;
      // Probes observe heads only. Direction loops own forwarding wakes through notifications
      // and their bounded missed-notification safety timers; a probe must not trigger membership.
      const directionalAfter = this.options.directionStallAfterMs ?? BRIDGE_DIRECTION_STALL_AFTER_MS;
      // Each cursor advance resets its own pending clock; progress in the other direction
      // cannot hide a wedge, and a backlog that keeps advancing is not a stall.
      if (pendingToRemote && now - this.#directionPendingSince.toRemote! >= directionalAfter) {
        direction = "toRemote";
        await this.#raiseStall(direction, `no toRemote acknowledgement for ${Math.floor(now - this.#directionPendingSince.toRemote!)} ms while toLocal ${pendingToLocal ? "has pending work" : "remains current"}`, true);
        return;
      }
      if (pendingToLocal && now - this.#directionPendingSince.toLocal! >= directionalAfter) {
        direction = "toLocal";
        await this.#raiseStall(direction, `no toLocal event receipt for ${Math.floor(now - this.#directionPendingSince.toLocal!)} ms while toRemote ${pendingToRemote ? "has pending work" : "remains current"}`, true);
        return;
      }

      const progressFreshMs = Math.max((this.options.watchdogIntervalMs ?? BRIDGE_WATCHDOG_INTERVAL_MS) * 2, timeoutMs * 2);
      const toRemoteWorking = !pendingToRemote || now - (this.#lastOkToRemoteMono ?? Number.NEGATIVE_INFINITY) <= progressFreshMs;
      const toLocalWorking = !pendingToLocal || now - (this.#lastOkToLocalMono ?? Number.NEGATIVE_INFINITY) <= progressFreshMs;
      // One live direction is enough to keep the service watchdog satisfied while the opposite
      // direction earns its longer independent stall deadline.
      if (toRemoteWorking || toLocalWorking) {
        this.#lastRoundTripAt = now;
        // A successful head probe is not event-delivery recovery. Only a real receipt
        // in the stalled direction (or restart) ends its alarm episode.
        this.options.notifyWatchdog?.();
        return;
      }
      const stalledFor = this.#lastRoundTripAt === undefined ? Infinity : Math.floor(now - this.#lastRoundTripAt);
      const stallAfter = this.options.watchdogStallAfterMs ?? BRIDGE_WATCHDOG_STALL_AFTER_MS;
      if (stalledFor >= stallAfter) {
        direction = pendingToRemote && pendingToLocal ? "both" : pendingToRemote ? "toRemote" : "toLocal";
        await this.#raiseStall(direction, `no successful round trip for ${stalledFor} ms`, false);
      }
    } catch {
      if (this.#stopped) return;
      const now = monotonicClock.now();
      const stalledFor = this.#lastRoundTripAt === undefined ? Infinity : Math.floor(now - this.#lastRoundTripAt);
      const stallAfter = this.options.watchdogStallAfterMs ?? BRIDGE_WATCHDOG_STALL_AFTER_MS;
      if (stalledFor >= stallAfter) await this.#raiseStall(direction, `no successful round trip for ${stalledFor} ms`, false);
    }
  }

  async #watchdogLoop(running: () => boolean): Promise<void> {
    while (running()) {
      await new Promise<void>(resolve => {
        const timer = setWatchdogTimeout(() => {
          if (this.#watchdogWake === wake) this.#watchdogWake = undefined;
          resolve();
        }, this.options.watchdogIntervalMs ?? BRIDGE_WATCHDOG_INTERVAL_MS);
        timer.unref?.();
        const wake = (): void => { clearWatchdogTimeout(timer); resolve(); };
        this.#watchdogWake = wake;
      });
      if (running()) await this.#watchdogProbe();
    }
  }

  /** Mirror each side's live roots into the other; concurrent callers share one pass. */
  syncPresence(): Promise<void> {
    if (this.#staleAdmission) return Promise.reject(this.#staleAdmission);
    if (this.#presenceSync) {
      this.options.debug?.("presence sync: joined in-flight pass");
      return this.#presenceSync;
    }
    const startedAt = this.options.debug ? performance.now() : 0;
    return this.#presenceSync = this.#syncPresence().catch((error: unknown) => {
      if (isBridgeRetryable(error)) this.#armPresenceRetry(LOCK_RETRY_MIN_MS);
      throw error;
    }).finally(() => {
      this.#presenceSync = undefined;
      this.options.debug?.(`presence sync: settled after ${performance.now() - startedAt} ms`);
    });
  }

  #armPresenceRetry(delayMs: number): void {
    this.#presenceRetryAt = Date.now() + Math.max(0, delayMs);
    // A direction may have started this mirror while run() was idle. Wake its loop so it
    // replaces the previous (possibly minutes-long) expiry timer with this retry deadline.
    this.#wake?.();
  }

  /** A pass already in flight may have sampled before this membership miss.
   * Settle it, then request/join one new sample before treating absence as definitive.
   * Failures still propagate; no polling or extra retry loop. */
  async #refreshMembership(): Promise<void> {
    const joined = this.#presenceSync !== undefined;
    await this.syncPresence();
    if (joined) await this.syncPresence();
  }

  async #syncPresence(): Promise<void> {
    const legacyRemote = this.options.remote instanceof RemoteBridgeSide ? this.options.remote : undefined;
    const hadLegacyNoBaseline = (legacyRemote?.legacyNoBaselineCycles ?? 0) > 0;
    const syncedAt = Date.now();
    const presenceVersion = this.#presenceVersion;
    const leaseVersion = this.#leaseVersion;
    const [local, claimed] = await Promise.all([this.options.local.presence(), this.options.remote.presence()]);
    const claimedValue: unknown = claimed;
    const claimedHosts = isObject(claimedValue) && Array.isArray(claimedValue.hosts)
      ? claimedValue.hosts.filter((host): host is BridgeHost => isObject(host)) : [];
    const remote = admitPresence(claimed, new Set(local.reserved), this.options.remoteName);
    if (this.#stopped) return;
    const relays = await this.options.local.relayPresence?.() ?? { hosts: [], participants: [] };
    const localRoots = { ...local, hosts: local.hosts.filter(({ record }) => !isNativeNonRootHost(record)) };
    const native = admitPresence(localRoots, new Set());
    const nativeNonRootHosts = local.hosts.filter(({ record }) => isNativeNonRootHost(record));
    const hosts = [...native.hosts, ...nativeNonRootHosts,
      ...relays.hosts.map(host => ({ ...host, record: { ...host.record, bridgeVia: this.options.localName } }))];
    const counts = new Map<string, number>();
    for (const { record } of hosts) if (!isNativeNonRootHost(record)) {
      counts.set(record.rootId, (counts.get(record.rootId) ?? 0) + 1);
    }
    // Never silently overwrite conflicting canonical duplicates from an adapter or relay.
    const outboundHosts = hosts.filter(({ record }) => isNativeNonRootHost(record) || counts.get(record.rootId) === 1);
    const outboundParticipants = [...native.participants, ...relays.participants.map(record => ({ ...record, bridgeVia: this.options.localName }))]
      .filter(record => counts.get(record.rootId) === 1);
    const outboundAdmissionDrops = hosts.length - outboundHosts.length +
      native.participants.length + relays.participants.length - outboundParticipants.length;
    const outbound = boundPresence({
      hosts: outboundHosts,
      participants: outboundParticipants,
      ...(native.dropped?.length ? { dropped: native.dropped } : {}),
      ...(native.partial || outboundAdmissionDrops > 0 ? { partial: true } : {}),
      ...(presenceDroppedCount(native) + outboundAdmissionDrops > 0
        ? { droppedCount: presenceDroppedCount(native) + outboundAdmissionDrops } : {}),
    });
    const partial = presenceIsPartial(local) || presenceIsPartial(claimed) || presenceIsPartial(native) ||
      presenceIsPartial(remote) || presenceIsPartial(outbound.presence);
    const droppedCount = presenceDroppedCount(remote) + presenceDroppedCount(outbound.presence);
    const outboundPresence: BridgeMirrorPresence = { ...outbound.presence,
      ...(partial ? { partial: true } : {}), ...(droppedCount > 0 ? { droppedCount } : {}) };
    const inboundPresence: BridgeMirrorPresence = { ...remote,
      ...(partial ? { partial: true } : {}), ...(droppedCount > 0 ? { droppedCount } : {}) };
    if (partial) this.#log(`presence: ${droppedCount} dropped records; this sync is partial, omissions held${outbound.dropped
      ? ` (${outbound.dropped} hosts pass the frame budget)` : ""}`);
    this.options.debug?.(`presence sync: inbound ${remote.hosts.length} hosts/${remote.participants.length} participants; outbound ${outbound.presence.hosts.length} hosts/${outbound.presence.participants.length} participants; partial=${partial} dropped=${droppedCount}`);
    // Both settle before a failure is reported, so no local write outlives this pass.
    const results = await Promise.allSettled([this.options.remote.mirror(outboundPresence), this.options.local.mirror(inboundPresence, syncedAt)]);
    // A transient failure must not hide a simultaneous permanent/transport failure.
    const failures = results.filter((result) => result.status === "rejected");
    const stale = failures.find(result => result.reason instanceof BridgeStaleIncarnationError);
    if (stale) {
      this.#staleAdmission = stale.reason as BridgeStaleIncarnationError;
      this.#ready = false;
    }
    const failure = failures.find((result) => !isBridgeRetryable(result.reason)) ?? failures[0];
    // Propagate once to the CLI's existing permanent-error log; never continue routing from
    // refused presence. stop() must fence this link without withdrawing newer authority.
    if (failure) throw failure.reason;
    // Retain only scalar deadlines from the latest admitted, sampled natives. A mirror commit
    // can outlast these while the origin keeps renewing (#10068). Missing held membership then
    // means bridge-stale, not proof of a foreign sender. This map can only defer, NEVER admit.
    this.#sampledDeadlines = new Map(remote.hosts.flatMap(({ record, expiresAt, observedAt }) =>
      remoteHostOf(record) === undefined && typeof observedAt === "number" && Number.isFinite(observedAt) &&
        Number.isFinite(expiresAt) && expiresAt > observedAt
        ? [[record.id, syncedAt + Math.min(BRIDGE_LEASE_MS, expiresAt - observedAt)] as const] : []));
    // A lock-delayed write must not make an old snapshot look freshly observed.
    this.#presenceAt = syncedAt;
    this.#presenceRetryAt = Infinity;
    this.#syncedPresenceVersion = presenceVersion;
    this.#syncedLeaseVersion = leaseVersion;
    // Origin updates normally renew mirrors. A source with a long lease and no heartbeat
    // still needs its capped, held projection to remain live: one lease deadline, 1 s before
    // the 15 s mirror cap (transport/commit headroom), not an unconditional 5 s presence loop.
    // Source expiry itself is reconciled once. No hosts means no deadline at all.
    const expiries = [...local.hosts, ...claimedHosts, ...relays.hosts]
      .map(host => typeof host.observedAt === "number" && Number.isFinite(host.observedAt)
        ? syncedAt + host.expiresAt - host.observedAt : host.expiresAt)
      .filter(expiry => Number.isFinite(expiry) && expiry > syncedAt);
    // Coalesce pending heartbeats for half the authority actually sampled, not half
    // the 15 s cap: a partly spent native TTL needs an earlier flush (#13645).
    this.#leaseRenewAt = syncedAt + Math.min(BRIDGE_LEASE_MS,
      ...expiries.map(expiry => expiry - syncedAt)) / 2;
    this.#expiryAt = Math.min(Infinity, ...expiries,
      ...(expiries.some(expiry => expiry > syncedAt + BRIDGE_LEASE_MS) ? [syncedAt + BRIDGE_LEASE_MS - 1_000] : []));
    // A completed mirror invalidates pending membership observations immediately, even when
    // no new event arrives. It releases the boundary, but is not itself a real wake: keep the
    // missed-notification safety backoff and do not bypass an existing direction backoff.
    for (const direction of ["toRemote", "toLocal"] as const) {
      if (this.#membershipRetryAt[direction] === undefined) continue;
      delete this.#membershipRetryAt[direction];
      this.#directionPending[direction] = true;
      if (this.#directionWakeReason[direction] !== "real") this.#directionWakeReason[direction] = "sync";
      if (!this.#directionBackoff[direction]) this.#directionWake[direction]?.("sync");
    }
    // Report after the mirror ACK as well as a skipped first legacy partial: this exposes recovery
    // immediately even for callers that use syncPresence() without the long-running loop.
    if (legacyRemote && (hadLegacyNoBaseline || legacyRemote.legacyNoBaselineCycles > 0)) this.#reportPresenceInfo();
  }

  /** Only an expired sampled projection makes these membership failures inconclusive. */
  #staleMembership(event: MeshEvent, rules: BridgeRules, direction: Direction, reason?: string): boolean {
    if (reason !== undefined && reason !== "sender is not a live participant of the remote" &&
      reason !== "ack target is not bound to this link" && !(reason === "not addressed across" && direction === "toRemote")) return false;
    // #refusal checks routing before body shape. A missing recipient must not hide a
    // structural violation; nor may an already-bridged work/echo become pending membership.
    if (!isIdentity(event.from) || (event.data !== undefined && !isObject(event.data)) ||
      this.#destinationRefusal(event, direction)) return false;
    const ids = direction === "toRemote" ? [event.to] : [event.from.id,
      ...(event.topic === "fabric.control.ack" ? [isObject(event.data) ? event.data.targetId : undefined] : [])];
    // ANY current native/other-link reservation defeats uncertainty, including an ACK's
    // other held id. Noncanonical/missing ACK targets are structural, not a stale mirror.
    const reserved = direction === "toRemote" ? rules.reservedRecipients : rules.reserved;
    if (!ids.every(id => typeof id === "string" && CANONICAL_ID.test(id) && !reserved?.has(id))) return false;
    return ids.some(id => (this.#sampledDeadlines.get(id as string) ?? Infinity) <= Date.now());
  }

  #deferMembership(direction: Direction, event: MeshEvent): void {
    const previous = this.#membershipSequence[direction];
    const initial = Math.min(this.options.membershipRetryMs ?? DEFAULT_MEMBERSHIP_RETRY_MS, MEMBERSHIP_RETRY_MAX_MS);
    const current = this.#membershipSafetyDelay[direction] ?? initial;
    const delay = this.#membershipAttempt[direction] === "safety"
      ? Math.min(current * 2, MEMBERSHIP_RETRY_MAX_MS) : current;
    this.#membershipRetryAt[direction] = Date.now() + delay;
    // A safety wake doubles only after it actually gets a retained pass. A grace wake and
    // sync completion preserve the current safety delay; a real wake clears it before here.
    this.#membershipSafetyDelay[direction] = delay;
    if (previous !== event.sequence) {
      this.options.debug?.(`${direction}: bridge membership pending for ${event.topic} ${event.id}; source cursor retained, nothing dropped`);
    }
    // Expiry alone never consumes the event. The next successful source sample decides
    // anew: stale evidence waits again; current absence or a proven foreign identity refuses.
    this.#membershipSequence[direction] = event.sequence;
  }

  #wakeRetainedMembership(direction: Direction): void {
    if (this.#membershipRetryAt[direction] === undefined && this.#membershipSequence[direction] === undefined) return;
    delete this.#membershipSafetyDelay[direction];
    this.#membershipRetryAt[direction] = Date.now();
  }

  #presenceDue(): boolean {
    if (Date.now() >= this.#presenceRetryAt) return true;
    if (!this.#notificationMode) return Date.now() - this.#presenceAt >= (this.options.presenceMs ?? DEFAULT_PRESENCE_MS);
    return this.#presenceVersion !== this.#syncedPresenceVersion || Date.now() >= this.#expiryAt ||
      (this.#leaseVersion !== this.#syncedLeaseVersion && Date.now() >= this.#leaseRenewAt) ||
      (this.#legacyPolling && Date.now() - this.#presenceAt >= (this.options.presenceMs ?? DEFAULT_PRESENCE_MS));
  }

  /** Only preemptive traffic refreshes may leave an active notification pass or its retained
   * independent mirror slot alone. Missing/expired membership and commit refusals still refresh. */
  #deferPreemptivePresence(): boolean {
    return this.#notificationMode && (this.#presenceSync !== undefined || this.options.remote.pendingMirrorTraffic === true);
  }

  /**
   * Who may be reached and who may speak across, read after the mirror step and again for every
   * page: only the peer's records this side holds now, still clear of every reserved hub id. A
   * claim refused or lost in this pass is never trusted (security review round 2, F1/F2).
   */
  async #authority(refreshIfStale = true): Promise<{ local: BridgePresence; remote: Pick<BridgePresence, "hosts" | "participants"> }> {
    if (this.#staleAdmission) throw this.#staleAdmission;
    const { local } = this.options;
    if (!local.owned) throw new Error("The hub side of a bridge must report the records it holds");
    // A page/read/write can wait on the mesh lock longer than a mirrored lease.
    // Refresh once before denying stale authority, sharing the normal presence pass.
    // Store lock and transport call timeouts bound it; no retry loop or fresh-message RPC.
    if (refreshIfStale && !this.#deferPreemptivePresence() && (this.#presenceDue() || this.#leaseVersion !== this.#syncedLeaseVersion ||
      (!this.#notificationMode && Date.now() - this.#presenceAt > DEFAULT_PRESENCE_MS))) await this.syncPresence();
    const [presence, owned] = await Promise.all([local.presence(), local.owned()]);
    return { local: presence, remote: admitPresence(owned, new Set(presence.reserved)) };
  }

  /** One pass: presence when due, then every new event in both directions. */
  async step(): Promise<BridgeStepResult> {
    if (!this.#cursor) await this.start();
    if (this.#presenceDue()) await this.syncPresence();
    if (this.#stopped) return { toRemote: 0, toLocal: 0, dropped: 0 };
    const toRemote = await this.#direction("toRemote");
    const toLocal = await this.#direction("toLocal");
    return { toRemote: toRemote.forwarded, toLocal: toLocal.forwarded, dropped: toRemote.dropped + toLocal.dropped };
  }

  #direction(direction: Direction): Promise<{ forwarded: number; dropped: number }> {
    return direction === "toRemote"
      ? this.#forward("toRemote", this.options.local, this.options.remote, async (refreshIfStale = true) => {
        const { local, remote } = await this.#authority(refreshIfStale);
        const relays = await this.options.local.relayPresence?.();
        return { recipients: remoteAddresses(remote), reservedRecipients: new Set(local.reserved),
          relayOrigins: new Map(relays?.hosts.map(host => [host.record.id, host.record.remoteHost!])) };
      })
      : this.#forward("toLocal", this.options.remote, this.options.local, async (refreshIfStale = true) => {
        const { local, remote } = await this.#authority(refreshIfStale);
        // The remote is untrusted: its sender (and an ack's target) must be bound to this link.
        const relays = await this.options.local.relayPresence?.();
        return { recipients: addresses(local), relayRecipients: remoteAddresses(relays ?? { hosts: [], participants: [] }),
          senders: senders(remote), identities: senderIdentities(remote), reserved: new Set(local.reserved),
          relayOrigins: new Map(relays?.hosts.map(host => [host.record.id, host.record.remoteHost!])) };
      });
  }

  /**
   * smarty-dev#9751: run()'s drain of one direction, independent of the other. Each loop is sequential (one page in
   * flight; pages are bounded by BRIDGE_PAGE_BYTES and the source's read limit), with its own cursor that moves only
   * after a publish succeeds, in order, so nothing in one direction (a slow page read, a stalled publish, a long
   * catch-up) can hold the other back. A retryable busy backs this direction off alone; any other error ends run()
   * as before (a closed transport fails both directions, and the supervisor restarts the bridge).
   */
  async #directionLoop(direction: Direction, running: () => boolean): Promise<void> {
    let retryMs = LOCK_RETRY_MIN_MS;
    let walCapDelay: number | undefined;
    let reported = false; // one log line per busy streak, as run() logs its own
    while (running()) {
      const wakeReason = this.#directionWakeReason[direction];
      delete this.#directionWakeReason[direction];
      // A notification can arrive during a pass, before the direction has a wait callback.
      // Reapply its release after that pass's deferral rather than losing it to a new timer.
      if (wakeReason === "real") this.#wakeRetainedMembership(direction);
      else if (wakeReason === "sync") delete this.#membershipRetryAt[direction];
      const retainedBefore = this.#membershipSequence[direction];
      const cursorBefore = this.#cursor?.[direction].after ?? 0;
      if (wakeReason === "safety" || wakeReason === "grace") this.#membershipAttempt[direction] = wakeReason;
      else delete this.#membershipAttempt[direction];
      this.#directionPending[direction] = false;
      this.#directionBackoff[direction] = false;
      this.#directionMirrorBackoff[direction] = false;
      const presenceVersion = this.#presenceVersion;
      let delay: number | undefined;
      let delayReason: DirectionWakeReason = "other";
      try {
        await this.#direction(direction);
        if (wakeReason === "safety" && retainedBefore !== undefined && retainedBefore > cursorBefore &&
            (this.#cursor?.[direction].after ?? cursorBefore) >= retainedBefore) {
          this.#unaddressed.safety += 1;
          this.#reportUnaddressed();
        }
        retryMs = LOCK_RETRY_MIN_MS;
        walCapDelay = undefined;
        reported = false;
        // ponytail: back off only retained work to recover a missed notification without idle polling.
        const membershipRetryAt = this.#membershipRetryAt[direction];
        if (membershipRetryAt !== undefined) {
          delay = Math.max(0, membershipRetryAt - Date.now());
          delayReason = "safety";
        }
        if (direction === "toLocal" && this.#factoryWaiting) {
          const factoryDelay = Math.max(0, this.#factoryRetryAt - this.#now());
          if (delay === undefined || factoryDelay < delay) { delay = factoryDelay; delayReason = "other"; }
        }
        if (this.#legacyPolling) {
          const pollDelay = this.options.pollMs ?? LEGACY_POLL_MS;
          if (delay === undefined || pollDelay < delay) { delay = pollDelay; delayReason = "other"; }
        }
      } catch (error) {
        if (!running()) return;
        if (isIdempotencyKeyReused(error) || !isBridgeRetryable(error)) {
          this.#notificationError ??= error instanceof Error ? error : new Error(String(error));
          this.#wake?.();
          return;
        }
        this.#directionMirrorBackoff[direction] = error instanceof BridgeMirrorDeferredError;
        // A late ACK may arrive while the other half of syncPresence is still settling.
        // Its already-latched wake must also bypass the mirror backoff installed here.
        this.#directionBackoff[direction] = !(this.#directionMirrorBackoff[direction] && this.#presenceVersion !== presenceVersion);
        if (error instanceof BridgeMirrorDeferredError || isMeshStateWalCap(error))
          delay = walCapDelay = walCapRetryDelay(walCapDelay, this.options.walCapRetryMinMs);
        else {
          // Equal jitter over the upper half of a capped exponential delay spreads independent bridges
          // without retrying sooner than the original deterministic backoff.
          const cap = Math.min(retryMs * 4, LOCK_RETRY_MAX_MS);
          delay = Math.floor(cap / 2 + Math.random() * cap / 2);
        }
        retryMs = Math.min(retryMs * 2, LOCK_RETRY_MAX_MS);
        this.#armPresenceRetry(delay);
        if (!reported) {
          this.#log(`${error instanceof BridgeMirrorDeferredError ? "bridge mirror degraded" : meshBusyLabel(error)} (${direction}); retrying in ${delay} ms: ${(error as Error).message}`);
          reported = true;
        }
      }
      delete this.#membershipAttempt[direction];
      if (!running()) return;
      if (this.#directionPending[direction] && !this.#directionBackoff[direction]) continue;
      await new Promise<void>((resolve) => {
        let timer: ReturnType<typeof setTimeout> | undefined;
        let graceTimer: ReturnType<typeof setTimeout> | undefined;
        const wake = (reason: DirectionWakeReason = "other"): void => {
          if (timer !== undefined) clearTimeout(timer);
          if (graceTimer !== undefined) clearTimeout(graceTimer);
          delete this.#directionWake[direction];
          this.#directionWakeReason[direction] = reason;
          resolve();
        };
        this.#directionWake[direction] = wake;
        if (delay !== undefined) timer = setTimeout(() => this.#directionWake[direction]?.(delayReason), Math.min(delay, 2_147_483_647));
        // RELAY-1 owns an exact persisted deadline, separate from the safety/lock backoff.
        // An expired grace is never rearmed when its source sender still lacks authority.
        const graceAt = this.#relayGraceAt(direction);
        if (graceAt !== undefined) graceTimer = setTimeout(() => wake("grace"), Math.min(Math.max(0, graceAt - Date.now()), 2_147_483_647));
      });
    }
  }

  async #forward(
    direction: "toRemote" | "toLocal",
    source: BridgeSide,
    target: BridgeSide,
    authority: (refreshIfStale?: boolean) => Promise<BridgeRules>,
  ): Promise<{ forwarded: number; dropped: number }> {
    const cursor = this.#cursor![direction];
    const seen = this.#seen[direction];
    let forwarded = 0;
    let dropped = 0;
    const retainedBoundary = this.#membershipSequence[direction];
    let passRefreshed = false;
    if (cursor.relayGrace && Number.isSafeInteger(cursor.relayGrace.sequence) && cursor.relayGrace.sequence <= cursor.after) {
      delete cursor.relayGrace;
      this.#save();
    }
    if ((this.#membershipSequence[direction] ?? Infinity) <= cursor.after) {
      delete this.#membershipSequence[direction];
      delete this.#membershipRetryAt[direction];
      delete this.#membershipSafetyDelay[direction];
    }
    const membershipRetryAt = this.#membershipRetryAt[direction];
    if (membershipRetryAt !== undefined) {
      const graceDue = this.#relayGraceExpired(direction);
      if (Date.now() < membershipRetryAt && !graceDue) return { forwarded, dropped };
      delete this.#membershipRetryAt[direction];
      // Expiration invalidates the observation, never the event: sample again before rereading
      // its unchanged source boundary. A pre-existing pass alone cannot prove fresh absence.
      // Failed/deferred mirrors retain that boundary too.
      await this.#refreshMembership();
      passRefreshed = true;
    }
    if (direction === "toLocal" && this.#factoryWaiting) {
      // A factory-rate deferral holds this link's inbound drain, in order, until its token is due;
      // until then a pass does not even reread the page.
      if (this.#now() < this.#factoryRetryAt) return { forwarded, dropped };
      this.#factoryWaiting = false;
    }
    // ponytail: a confirmed short receipt constrains later speculation in this pass.
    // All holds stay fresh; even a transient short receipt keeps the remainder small.
    // The next #forward pass starts at the full batch limit again.
    let candidateLimit = BRIDGE_BATCH_MAX_EVENTS;
    while (true) {
      const start = cursor.after;
      const startOffset = cursor.offset;
      const page = await (source.tail?.(start, startOffset) ?? source.read(start));
      const hasControlEvents = page.events.some(event => event.topic === "fabric.control.command" || event.topic === "fabric.control.ack");
      const sourceHead = hasControlEvents ? await source.latestSequence() : page.through;
      // Even filtered/skip-only reads can carry this drain past the mirrors' lease.
      // Reconcile latched origin changes/expiry even on a filtered page.
      const retainedRefresh = passRefreshed && retainedBoundary !== undefined && cursor.after < retainedBoundary &&
        this.#presenceVersion === this.#syncedPresenceVersion && this.#leaseVersion === this.#syncedLeaseVersion;
      if (!retainedRefresh && !this.#deferPreemptivePresence() && this.#presenceDue()) {
        await this.syncPresence();
        passRefreshed = true;
      }
      // Reuse only this retained boundary's completed sample; real changes and later pages
      // still refresh normally. Canonical owned()/holds() checks are never skipped.
      let rules: BridgeRules = page.events.length ? await authority(!retainedRefresh) : { recipients: new Set<string>() };
      for (const skip of Array.isArray(page.skipped) ? page.skipped : []) {
        dropped += 1;
        this.#log(`${direction}: skipped ${skip.topic} ${skip.id} (sequence ${skip.sequence}): ${skip.bytes} bytes pass the ${BRIDGE_PAGE_BYTES}-byte frame budget`);
      }
      let fallbackBatchThrough = cursor.after;
      for (let index = 0; index < page.events.length; index++) {
        const event = page.events[index]!;
        if (event.sequence <= cursor.after) continue;
        if (this.#stopped) return { forwarded, dropped };
        // Keep receipt-backed controls in their own batches: a command conflict can leave a
        // committed prefix only when every prefix event has its own destination receipt.
        const batchUsesReceipt = event.topic === "fabric.control.command" || event.topic === "fabric.control.ack";
        if (target.publishBatch && candidateLimit > 1 && event.sequence > fallbackBatchThrough &&
          page.events.length - index > 1 && !seen.has(event.id) &&
          !isRelayReceipt(event) && !this.#isRelayCommand(event, rules, direction)) {
          if (!this.#deferPreemptivePresence() && this.#presenceDue()) {
            await this.syncPresence();
            passRefreshed = true;
            rules = await authority(false);
          }
          if (this.#stopped) return { forwarded, dropped };
          const candidates: Array<{ event: BridgePublish; held: string[] }> = [];
          // Which candidates spend a factory token; the batch ends at the first one over the rate,
          // and the single-event path below defers it.
          // Each candidate's factory cost in bytes (0 when it spends nothing).
          const spends: number[] = [];
          let spending = 0;
          let spendingBytes = 0;
          let bytes = 0;
          for (const candidate of page.events.slice(index, index + candidateLimit)) {
            const candidateUsesReceipt = candidate.topic === "fabric.control.command" || candidate.topic === "fabric.control.ack";
            if (candidate.sequence <= cursor.after || seen.has(candidate.id) || isRelayReceipt(candidate) ||
              candidateUsesReceipt !== batchUsesReceipt || this.#isRelayCommand(candidate, rules, direction) ||
              this.#refusal(candidate, rules, direction)) break;
            if (direction === "toRemote" && this.options.local.holds && !this.options.local.holds(candidate.to!)) break;
            const data = isObject(candidate.data) ? candidate.data : {};
            const item = {
              event: this.#outbound(candidate, rules, direction),
              held: direction === "toLocal" ? [candidate.from.id,
                ...(candidate.topic === "fabric.control.ack" && typeof data.targetId === "string" ? [data.targetId] : [])] : [],
            };
            const size = Buffer.byteLength(JSON.stringify(item.event), "utf8");
            const spend = this.#viaFactory(candidate, rules, direction);
            if (spend && !this.#factoryAffords(spending, spendingBytes, size)) break;
            bytes += size + 1;
            if (bytes > BRIDGE_PAGE_BYTES - 1024) break;
            candidates.push(item);
            spends.push(spend ? size : 0);
            if (spend) {
              spending += 1;
              spendingBytes += size;
            }
          }
          if (candidates.length > 1) {
            let committed: readonly BridgePublishReceipt[] | undefined;
            try {
              committed = await this.#deliveryAge.trackBatch(direction, page.events.slice(index, index + candidates.length),
                () => target.publishBatchWithReceipt ? target.publishBatchWithReceipt(candidates) : target.publishBatch!(candidates),
                Math.max(0, sourceHead - cursor.after));
            }
            catch (error) {
              // A key conflict falls through to per-event handling for this whole batch. Its
              // receipt-backed control prefix is safe to replay; do not retry overlapping suffix batches.
              if (isIdempotencyKeyReused(error)) fallbackBatchThrough = page.events[index + candidates.length - 1]!.sequence;
              else if (!(error instanceof BridgeOwnershipError) && !isPermanent(error)) throw error;
            }
            if (committed) {
              if (!Array.isArray(committed) || !committed.length || committed.length > candidates.length ||
                  committed.some(result => !Number.isSafeInteger(result?.sequence) || result.sequence <= 0)) {
                throw new Error("Invalid bridge batch receipt");
              }
              if (committed.length < candidates.length) candidateLimit = Math.min(candidateLimit, committed.length);
              const spent = spends.slice(0, committed.length).filter((size) => size > 0);
              this.#factoryTokens -= spent.length;
              this.#factoryBytes -= spent.reduce((sum, size) => sum + size, 0);
              const ids = page.events.slice(index, index + committed.length).map(item => item.id);
              for (const id of ids) seen.add(id);
              cursor.mark = Math.max(cursor.mark, ...committed.map(result => result.sequence));
              cursor.after = page.events[index + committed.length - 1]!.sequence;
              forwarded += committed.length;
              this.#save(); // Advance only after the destination confirms the prefix durably.
              this.#clearStallOnProgress(direction);
              for (const id of ids) seen.delete(id);
              this.#reportUnaddressed();
              index += committed.length - 1;
              // Locks are released and the durable prefix is checkpointed. Yield a real event-loop
              // turn (not a microtask) before reacquiring, so local writers and transport I/O run.
              await new Promise<void>(resolve => setImmediate(resolve));
              continue;
            }
          }
        }
        let refreshed = passRefreshed && retainedBoundary === event.sequence;
        const refresh = async (): Promise<void> => {
          refreshed = true;
          passRefreshed = true;
          await this.#refreshMembership();
          // At most the pre-existing pass plus one new snapshot; then canonical authority.
          rules = await authority(false);
        };
        let reason = this.#refusal(event, rules, direction);
        const lapsedId = reason === "sender is not a live participant of the remote" ? event.from.id
          : reason === "ack target is not bound to this link" && isObject(event.data) && typeof event.data.targetId === "string" ? event.data.targetId
          : reason === "not addressed across" && direction === "toRemote" ? event.to : undefined;
        // A file notification may still be queued when a later event append is already
        // readable. Refresh a newly observed canonical address once before dropping it;
        // admission/holds still use the committed projection, never the sender's claim.
        if (!refreshed && lapsedId && (this.options.local.mirrored?.(lapsedId) ||
          (this.#notificationMode && CANONICAL_ID.test(lapsedId)))) {
          await refresh();
          reason = this.#refusal(event, rules, direction);
        }
        if (reason && this.#staleMembership(event, rules, direction, reason)) {
          this.#deferMembership(direction, event);
          if (cursor.after > start) this.#save();
          return { forwarded, dropped };
        }
        // Cross-spoke commands never enter either hub body-publication seam. Even a
        // missing/lapsed destination gets its refusal only on the original source link.
        const relayCommand = this.#isRelayCommand(event, rules, direction);
        const relaySenderAllowed = relayCommand && this.#relaySenderAllowed(event, rules);
        // If both ends are stale, retain the source uncertainty first. The destination
        // observation is only a bounded wait signal; it never admits the sender.
        if (relayCommand && this.#relayDestinationStale(event, rules) && !relaySenderAllowed) {
          this.#relayGracePending(event);
          this.#deferMembership(direction, event);
          if (cursor.after > start) this.#save();
          return { forwarded, dropped };
        }
        if (relayCommand && relaySenderAllowed) {
          if (!seen.has(event.id)) {
            if (!this.#deferPreemptivePresence() && this.#presenceDue()) await refresh();
            const result = await this.#relayCommand(event, rules);
            if (result.pending) {
              this.#deferMembership(direction, event);
              if (cursor.after > start) this.#save();
              return { forwarded, dropped };
            }
            if (result.delivered) {
              if (result.sequence !== undefined) cursor.mark = Math.max(cursor.mark, result.sequence);
              forwarded += 1;
              this.#clearStallOnProgress(direction);
            } else dropped += 1;
            this.#clearRelayGrace(direction, event);
          }
          cursor.after = event.sequence;
          this.#save();
          seen.delete(event.id);
          await new Promise<void>(resolve => setImmediate(resolve));
          continue;
        }
        if (reason) {
          if (reason !== "not addressed across") {
            dropped += 1;
            this.#log(`${direction}: dropped ${event.topic} ${event.id} from ${event.from.id}: ${reason}`);
          } else this.#notAddressed(direction, event);
        } else if (this.#stopped) {
          return { forwarded, dropped };
        } else if (!seen.has(event.id)) {
          // A long backlog still reconciles origin updates; a lapsed held mirror gets the
          // existing one-refresh commit refusal path, never an idle renewal timer.
          if (!this.#deferPreemptivePresence() && this.#presenceDue()) {
            await this.syncPresence();
            passRefreshed = true;
          }
          if (this.#stopped) return { forwarded, dropped };
          let factorySpend = 0;
          if (this.#viaFactory(event, rules, direction)) {
            const size = Buffer.byteLength(JSON.stringify(this.#outbound(event, rules, direction)), "utf8");
            if (!this.#factoryAffords(0, 0, size)) {
              // Defer, never drop (pi-fabric#876 review, P1-2): this event and all after it stay
              // unread. The cursor stays before it (offset at this page's start; a reread skips
              // by sequence), so a restart or the next drain resumes exactly here.
              this.#deferFactory(event, size);
              if (cursor.after > start) this.#save();
              return { forwarded, dropped };
            }
            factorySpend = size;
          }
          try {
            // Revalidated for each event after the awaits before it: an inbound event commits only
            // while its sender (and an ack's target) is still this link's; an outbound one is sent
            // only while its recipient is (security review round 3, F2).
            const held = direction === "toLocal"
              ? [event.from.id, ...(event.topic === "fabric.control.ack" && isObject(event.data) && typeof event.data.targetId === "string" ? [event.data.targetId] : [])]
              : [];
            const publish = async (): Promise<BridgePublishReceipt> => {
              if (direction === "toRemote" && this.options.local.holds && !this.options.local.holds(event.to!)) {
                throw new BridgeOwnershipError(`${event.to} is no longer bound to bridge link ${this.options.remoteName}`);
              }
              // Recheck the captured destination at the last seam, including after a refresh.
              const destinationRefusal = this.#destinationRefusal(event, direction);
              if (destinationRefusal) throw new BridgeOwnershipError(destinationRefusal);
              // Built from the current rules, so a sender identity refreshed below is the one sent.
              const outbound = this.#outbound(event, rules, direction);
              return target.publishWithReceipt ? target.publishWithReceipt(outbound, held) : target.publish(outbound, held);
            };
            let published: BridgePublishReceipt;
            try {
              published = await this.#deliveryAge.track(direction, event, publish, Math.max(0, sourceHead - cursor.after));
            } catch (error) {
              // Ownership errors happen before append, under the publish lock. A mirror can
              // lapse during that wait: refresh once, reread authority, then retry once.
              const ids = direction === "toLocal" ? held : [event.to!];
              if (isIdempotencyKeyReused(error) || !(error instanceof BridgeOwnershipError) || refreshed || this.#destinationRefusal(event, direction) ||
                  !ids.some((id) => this.options.local.mirrored?.(id))) throw error;
              await refresh();
              if (this.#stopped) return { forwarded, dropped };
              if (this.#refusal(event, rules, direction)) throw error;
              published = await this.#deliveryAge.track(direction, event, publish, Math.max(0, sourceHead - cursor.after));
            }
            if (factorySpend > 0) {
              this.#factoryTokens -= 1;
              this.#factoryBytes -= factorySpend;
            }
            seen.add(event.id);
            cursor.mark = Math.max(cursor.mark, published.sequence);
            cursor.after = event.sequence;
            forwarded += 1;
            this.#save();
            this.#clearStallOnProgress(direction);
            // Only this newly inserted ID is covered by the checkpoint; recovery IDs stay.
            seen.delete(event.id);
            this.#reportUnaddressed();
            // Legacy/singleton drains must also give waiting writers a turn after checkpoint.
            await new Promise<void>(resolve => setImmediate(resolve));
          } catch (error) {
            if (error instanceof BridgeOwnershipError) {
              // Keep the two-attempt/one-refresh bound. An origin can renew throughout BOTH
              // lock waits; expiry is not a foreign identity. Recheck current reservations so
              // a proven takeover still refuses, and otherwise retry from the durable cursor.
              rules = await authority(false);
              const currentReason = this.#refusal(event, rules, direction);
              if (this.#staleMembership(event, rules, direction, currentReason)) {
                this.#deferMembership(direction, event);
                if (cursor.after > start) this.#save();
                return { forwarded, dropped };
              }
            }
            // A key conflict belongs to this event, not the link. It is permanently dropped;
            // other structural/permanent refusals keep their existing behavior.
            if (isIdempotencyKeyReused(error)) {
              dropped += 1;
              this.#logIdempotencyConflict(direction, event, error);
            } else {
              if (!isPermanent(error)) throw error;
              dropped += 1;
              this.#log(`${direction}: refused ${event.topic} ${event.id}: ${(error as Error).message}`);
            }
          }
        }
        cursor.after = event.sequence;
      }
      // The page may end in events the allow-list filtered out; a page that moved may have more.
      cursor.after = Math.max(cursor.after, page.through);
      if (page.offset !== undefined) cursor.offset = page.offset;
      else delete cursor.offset;
      const moved = cursor.after > start || cursor.offset !== startOffset;
      if (moved) this.#save();
      if (!moved) break;
    }
    return { forwarded, dropped };
  }

  #isRelayCommand(event: MeshEvent, rules: BridgeRules, direction: Direction): boolean {
    return direction === "toLocal" && event.topic === "fabric.control.command" && typeof event.to === "string" &&
      CANONICAL_ID.test(event.to) && !rules.recipients.has(event.to);
  }

  #relaySenderAllowed(event: MeshEvent, rules: BridgeRules): boolean {
    return !hasBridgeField(event) && !isRelayReceipt(event) && isIdentity(event.from) && isObject(event.data) &&
      !rules.reserved?.has(event.from.id) && rules.senders?.has(event.from.id) === true && rules.identities?.has(event.from.id) === true;
  }

  #staleRelaySender(event: MeshEvent, rules: BridgeRules): boolean {
    if (hasBridgeField(event) || isRelayReceipt(event) || !isIdentity(event.from) || !isObject(event.data) ||
        !CANONICAL_ID.test(event.from.id) || rules.reserved?.has(event.from.id)) return false;
    return (this.#sampledDeadlines.get(event.from.id) ?? Infinity) <= Date.now();
  }

  #relayDestinationStale(event: MeshEvent, rules: BridgeRules): boolean {
    if (typeof event.to !== "string" || !isObject(event.data) ||
        (!this.#relaySenderAllowed(event, rules) && !this.#staleRelaySender(event, rules))) return false;
    const origin = this.options.local.relayStaleOrigin?.(event.to);
    return origin !== undefined && event.data.destinationRemoteHost === origin;
  }

  #relayGraceAt(direction: Direction): number | undefined {
    if (direction !== "toLocal") return undefined;
    const retained = this.#cursor?.toLocal.relayGrace;
    if (!isObject(retained) || !Number.isSafeInteger(retained.sequence) || retained.sequence <= (this.#cursor?.toLocal.after ?? 0) ||
        !Number.isFinite(retained.startedAt) || retained.startedAt < 0) return undefined;
    const deadline = retained.startedAt + BRIDGE_LEASE_MS;
    return Number.isFinite(deadline) && Date.now() < deadline ? deadline : undefined;
  }

  #relayGraceExpired(direction: Direction): boolean {
    if (direction !== "toLocal") return false;
    const retained = this.#cursor?.toLocal.relayGrace;
    if (!isObject(retained) || !Number.isSafeInteger(retained.sequence) || retained.sequence <= (this.#cursor?.toLocal.after ?? 0) ||
        !Number.isFinite(retained.startedAt) || retained.startedAt < 0) return false;
    const deadline = retained.startedAt + BRIDGE_LEASE_MS;
    return Number.isFinite(deadline) && Date.now() >= deadline;
  }

  #relayGracePending(event: MeshEvent): boolean {
    const cursor = this.#cursor!.toLocal;
    const retained = cursor.relayGrace;
    const wallNow = Date.now();
    const now = Number.isFinite(wallNow) && wallNow >= 0 ? wallNow : 0;
    const sameEvent = isObject(retained) && Number.isSafeInteger(retained.sequence) && retained.sequence > 0 &&
      retained.sequence === event.sequence && (retained.id === undefined || retained.id === event.id);
    const validStart = isObject(retained) && Number.isFinite(retained.startedAt) && retained.startedAt >= 0 &&
      retained.startedAt <= now && Number.isFinite(retained.startedAt + BRIDGE_LEASE_MS);
    // Repair legacy/malformed/future records from the local clock, never a peer timestamp.
    if (!sameEvent || !validStart) {
      cursor.relayGrace = { sequence: event.sequence, startedAt: now, id: event.id };
      this.#save();
      return true;
    }
    return now < retained.startedAt + BRIDGE_LEASE_MS;
  }

  #clearRelayGrace(direction: Direction, event: MeshEvent): void {
    const cursor = this.#cursor![direction];
    const retained = cursor.relayGrace;
    if (retained?.sequence === event.sequence && (retained.id === undefined || retained.id === event.id)) {
      delete cursor.relayGrace;
    }
  }

  async #relayCommand(event: MeshEvent, rules: BridgeRules): Promise<{ delivered: boolean; sequence?: number; pending?: boolean }> {
    const { local, remote, relay, localName, remoteName } = this.options;
    const pub = this.#outbound(event, rules, "toLocal");
    const destination = rules.relayOrigins?.get(pub.to);
    const senderHeld = local.holds?.(pub.from.id, pub.from);
    // Do not turn a source sender's expired sampled projection into an unavailable ACK
    // and consume its command. This is a wait, not permission to bypass the sender fence.
    if (!senderHeld &&
      (this.#staleMembership(event, rules, "toLocal", this.#refusal(event, rules, "toLocal")) ||
        (this.#relayDestinationStale(event, rules) && this.#staleRelaySender(event, rules)))) return { delivered: false, pending: true };
    const staleDestination = this.#relayDestinationStale(event, rules);
    if (staleDestination && this.#relayGracePending(event)) return { delivered: false, pending: true };
    try {
      if (this.#stopped || !this.#ready || this.#notificationError || this.#refusal(event, rules, "toLocal") || !relay ||
        !destination || destination === remoteName || destination === localName ||
        pub.data.destinationRemoteHost !== destination || local.relayOrigin?.(pub.to) !== destination ||
        !senderHeld) throw relayUnavailable();
      await relay(destination, pub);
    } catch (error) {
      // A destination can lapse after the first admission but before its own held fence.
      // The same bounded grace defers, never routes, that stale observation.
      if (!isIdempotencyKeyReused(error) && this.#relayDestinationStale(event, rules) && this.#relayGracePending(event)) {
        return { delivered: false, pending: true };
      }
      // Timeout remains ambiguous. A typed conflict is a fixed, nonretryable source-local
      // refusal; neither case exposes owner assertions, arbitrary errors, body or hash.
      const idempotencyConflict = isIdempotencyKeyReused(error);
      const refusal = idempotencyConflict
        ? { error: "Fabric idempotency key reused with different input", errorCode: FABRIC_IDEMPOTENCY_KEY_REUSED, retryable: false }
        : { error: "Relay destination unavailable", errorCode: RELAY_UNAVAILABLE_CODE, retryable: true };
      await remote.publish({
        topic: "fabric.control.ack", kind: "ack",
        from: { id: `bridge:${localName}`, name: "mesh-bridge", kind: "main" }, to: pub.from.id,
        data: { version: 1, commandId: pub.data.commandId, targetId: pub.data.targetId,
          accepted: false, ...refusal,
          bridge: { from: localName, id: event.id } },
      });
      if (idempotencyConflict) this.#logIdempotencyConflict("toLocal", event, error);
      return { delivered: false };
    }
    // Only a confirmed destination receipt permits a hub envelope. Its sequence is LOCAL;
    // the destination sequence is deliberately absent from the delivery-only relay API.
    let receipt: { sequence: number } | undefined;
    try { receipt = await local.recordRelay?.(pub, [pub.from.id]); }
    catch (error) {
      if (!(error instanceof BridgeOwnershipError)) throw error;
      const { local: presence, remote: owned } = await this.#authority(false);
      const current = { ...rules, senders: senders(owned), identities: senderIdentities(owned), reserved: new Set(presence.reserved) };
      if (!this.#staleMembership(event, current, "toLocal", this.#refusal(event, current, "toLocal"))) throw error;
      // Destination dedupe covers retry after delivery but before the fenced hub receipt.
      return { delivered: false, pending: true };
    }
    if (receipt && (!Number.isSafeInteger(receipt.sequence) || receipt.sequence <= 0)) throw new Error("Invalid relay receipt");
    return { delivered: true, ...(receipt ? { sequence: receipt.sequence } : {}) };
  }

  /** An inbound ACK for a command this link carried out, addressed to that command's replyTo (smarty-dev#9751). */
  #awaitedAck(event: MeshEvent): boolean {
    const commandId = isObject(event.data) ? event.data.commandId : undefined;
    if (event.topic !== "fabric.control.ack" || typeof commandId !== "string" || !isObject(event.data)) return false;
    const awaited = this.#awaitingAck.get(commandId);
    // Only from the owner the command was addressed to, about the same target, to the sender itself.
    return awaited !== undefined && awaited.expiresAt > this.#now() && awaited.replyTo === event.to &&
      awaited.target === event.from.id && awaited.targetId === event.data.targetId;
  }

  #destinationRefusal(event: MeshEvent, direction: "toRemote" | "toLocal"): string | undefined {
    // This is control-plane routing metadata, not a business field on work events or ACKs.
    // Absence alone is legacy compatibility; an explicitly present undefined is malformed.
    if (event.topic !== "fabric.control.command" || !isObject(event.data) ||
        !Object.hasOwn(event.data, "destinationRemoteHost")) return undefined;
    const destination = event.data.destinationRemoteHost;
    if (destination === null) return "command destination is native and must not cross a bridge";
    if (typeof destination !== "string") return "command destinationRemoteHost is not a string or null";
    const intended = direction === "toRemote" ? this.options.remoteName
      : (typeof event.to === "string" ? this.options.local.relayOrigin?.(event.to) : undefined) ?? this.options.localName;
    if (destination !== intended) return `command destination is not bound to bridge link ${intended}`;
    return undefined;
  }

  #refusal(event: MeshEvent, rules: BridgeRules, direction: "toRemote" | "toLocal"): string | undefined {
    if (!isBridgedTopic(event) || isRelayReceipt(event) ||
      (event.verification === "bridge" && event.topic === "fabric.control.command" && !isObject(event.data))) return "not allowed";
    if (hasBridgeField(event)) {
      const stamp = bridgeStampOf(event);
      if (direction !== "toRemote" || event.verification !== "bridge" || !stamp || hasHubHop(event) ||
          (event.topic !== "fabric.control.command" && event.topic !== "fabric.control.ack") ||
          stamp.from === this.options.remoteName || rules.relayOrigins?.get(event.from.id) !== stamp.from) return "not allowed";
    }
    if (direction === "toLocal" && typeof event.to === "string" && rules.relayOrigins?.has(event.to) &&
        event.topic !== "fabric.control.command" && event.topic !== "fabric.control.ack") return "not allowed";
    // The hub factory reads by topic: addressed work/owner wakes and unaddressed WAIT/ETA issue
    // records cross within its rate bound. The latter gain `to: factory` only on the hub copy
    // (smarty-dev#9349); no other broadcast does, and never hub to remote.
    if (!((typeof event.to === "string" && (rules.recipients.has(event.to) || rules.relayRecipients?.has(event.to))) ||
      (direction === "toLocal" && (isHubFactoryEvent(event) || this.#awaitedAck(event))))) {
      return "not addressed across";
    }
    if (event.data !== undefined && !isObject(event.data)) return "data is not an object";
    const destinationRefusal = this.#destinationRefusal(event, direction);
    if (destinationRefusal) return destinationRefusal;
    if (!isIdentity(event.from)) return "no sender identity";
    if (rules.reserved?.has(event.from.id)) return "sender claims a hub identity";
    if (rules.senders && !rules.senders.has(event.from.id)) return "sender is not a live participant of the remote";
    if (rules.identities && !rules.identities.has(event.from.id)) return "sender is not a live participant of the remote";
    if (rules.senders && event.topic === "fabric.control.ack") {
      const target = isObject(event.data) ? event.data.targetId : undefined;
      if (typeof target !== "string" || !rules.senders.has(target)) return "ack target is not bound to this link";
    }
    return undefined;
  }

  /** Follow notifications until stop() or failure. No timer is armed for empty idle.
   * Timer inventory: source/held-mirror expiry and pending half-lease flush (one shot);
   * retained stale-membership safety net (1 s..30 s backoff), plus exact persisted relay grace;
   * actual lock/SQLite busy, including WAL notification commit settlement (100 ms..2 s);
   * WAL-cap refusal (1 s..30 s); explicitly negotiated legacy-only 250 ms polling.
   * Held-publication fence retries, RPC deadlines and stop
   * bounds elsewhere are also finite pending work, not ingress/presence polling. */
  async run(): Promise<void> {
    const unsubscribers: Array<() => void> = [];
    let started = false;
    let retryMs = LOCK_RETRY_MIN_MS;
    let reportedTimeout = false;
    let walCapLoggedAt = Number.NEGATIVE_INFINITY;
    let walCapDelay: number | undefined;
    let backingOff = false;
    let mirrorBackingOff = false;
    // smarty-dev#9751: once start() has loaded the cursors, each direction runs in its own loop (#directionLoop),
    // which also does that direction's catch-up: no sequential drain first.
    let ended = false;
    const running = (): boolean => !this.#stopped && !ended;
    const wakeDirections = (retainedOnly = false): void => {
      for (const direction of ["toRemote", "toLocal"] as const) {
        if (retainedOnly && (this.#membershipSequence[direction] ?? 0) <= (this.#cursor?.[direction].after ?? 0)) continue;
        this.#wakeRetainedMembership(direction);
        this.#directionPending[direction] = true;
        this.#directionWakeReason[direction] = "real";
        if (!this.#directionBackoff[direction]) this.#directionWake[direction]?.("real");
      }
    };
    this.#notificationMode = true;
    const changed = (kind: BridgeChange): void => {
      if (kind === "lease") {
        const firstPending = this.#leaseVersion === this.#syncedLeaseVersion;
        this.#leaseVersion++;
        // One pending half-lease flush coalesces every lane's heartbeat. Further renewals
        // neither reschedule it nor take a lock; actual routed work refreshes immediately.
        if (firstPending && !backingOff) this.#wake?.();
        wakeDirections(true);
        return;
      }
      this.#changesPending = true;
      if (kind === "presence") {
        this.#presenceVersion++;
        // A late mirror ACK already emits presence in RemoteBridgeSide. Do not leave it
        // waiting behind the degraded-mirror backoff (up to 30 s). WAL/lock backoff is intact.
        if (mirrorBackingOff) backingOff = false;
        for (const direction of ["toRemote", "toLocal"] as const) {
          if (this.#directionMirrorBackoff[direction]) this.#directionBackoff[direction] = false;
        }
      }
      if (!backingOff) this.#wake?.();
      wakeDirections();
    };
    const failed = (error: Error): void => {
      this.#notificationError ??= error;
      this.#wake?.();
    };
    try {
      while (!this.#stopped) {
        if (this.#notificationError) throw this.#notificationError;
        this.#changesPending = false; // changes during any await below stay latched
        const presenceVersion = this.#presenceVersion;
        const pass = (async () => {
          const sides = [this.options.local, this.options.remote];
          while (unsubscribers.length < sides.length && !this.#stopped) {
            const side = sides[unsubscribers.length]!;
            if (!side.subscribeChanges && unsubscribers.length === 0) throw new Error("The local bridge side requires filesystem change notifications");
            const unsubscribe = await side.subscribeChanges?.(changed, failed);
            if (!unsubscribe) {
              if (unsubscribers.length === 0) throw new Error("The local bridge side requires filesystem change notifications");
              if (side.protocolVersion !== LEGACY_BRIDGE_PROTOCOL_VERSION) {
                throw new Error(`Bridge peer protocol ${String(side.protocolVersion)} has no change subscription; refusing unversioned/current notification downgrade`);
              }
              this.#legacyPolling = true;
              this.#log(`legacy peer protocol ${side.protocolVersion}: transitional 250 ms polling until fleet-wide notification-capable install; no events are discarded`);
            }
            unsubscribers.push(unsubscribe ?? (() => undefined));
          }
          if (this.#stopped) return;
          if (!started) {
            if (!this.#ready) await this.start();
            else {
              // Subscription setup can race an append: recover IDs again without
              // resetting already-ready cursors or losing their committed marks.
              await this.#reconcileSeen();
              await this.syncPresence();
            }
          } else if (this.#presenceDue()) await this.syncPresence();
        })();
        // stop() needs a settlement fence, not a second rejection of a failed pass.
        this.#running = pass.then(() => undefined, () => undefined);
        let delay: number | undefined;
        backingOff = false;
        try {
          await pass;
          mirrorBackingOff = false;
          reportedTimeout = false;
          walCapDelay = undefined;
          retryMs = LOCK_RETRY_MIN_MS;
          if (!started) {
            started = true;
            this.#directions = Promise.all([this.#directionLoop("toRemote", running), this.#directionLoop("toLocal", running)])
              .then(() => undefined, () => undefined);
            this.#watchdogTask = this.#watchdogLoop(running);
          }
          const remote = this.options.remote instanceof RemoteBridgeSide ? this.options.remote : undefined;
          const noBaselineSince = remote?.legacyNoBaselineSince;
          const legacyInfoAt = remote && remote.legacyNoBaselineCycles > 0 && !this.#legacyNoBaselineOverdueReported &&
            noBaselineSince !== undefined
            ? noBaselineSince + LEGACY_PARTIAL_HOLD_MS + 1 : Infinity;
          const noBaselineAlarmAt = remote && supportsLegacyBaselineAlarm(remote.supportsPartialPresence) &&
            remote.legacyNoBaselineCycles > 0 && remote.legacyNoBaselineSinceMonotonic !== undefined && this.options.publishLegacyBaselineAlarm
            ? this.#legacyBaselineAlarm.nextAttemptAt(remote.legacyNoBaselineSinceMonotonic) ?? Infinity : Infinity;
          const deadline = Math.min(this.#presenceRetryAt, this.#expiryAt,
            this.#leaseVersion !== this.#syncedLeaseVersion ? this.#leaseRenewAt : Infinity, legacyInfoAt);
          if (Number.isFinite(deadline)) delay = Math.max(0, deadline - Date.now());
          if (Number.isFinite(noBaselineAlarmAt)) {
            delay = Math.min(delay ?? Infinity, Math.max(0, noBaselineAlarmAt - monotonicClock.now()));
          }
          if (this.#legacyPolling) delay = Math.min(delay ?? Infinity, this.options.pollMs ?? LEGACY_POLL_MS);
        } catch (error) {
          if (this.#stopped) return;
          if (!isBridgeRetryable(error)) throw error;
          mirrorBackingOff = error instanceof BridgeMirrorDeferredError;
          backingOff = !(mirrorBackingOff && this.#presenceVersion !== presenceVersion);
          delay = retryMs;
          retryMs = Math.min(retryMs * 2, LOCK_RETRY_MAX_MS);
          if (error instanceof BridgeMirrorDeferredError || isMeshStateWalCap(error)) {
            walCapDelay = walCapRetryDelay(walCapDelay, this.options.walCapRetryMinMs);
            delay = walCapDelay;
            const now = Date.now();
            if (now - walCapLoggedAt >= WAL_CAP_LOG_MS) {
              this.#log(`${error instanceof BridgeMirrorDeferredError ? "bridge mirror degraded" : "mesh state WAL cap"}; retrying in ${delay} ms: ${meshDeferredSummary(error)}`); // no reader pids
              walCapLoggedAt = now;
            }
          } else if (!reportedTimeout) {
            this.#log(`${meshBusyLabel(error)}; retrying in ${delay} ms: ${(error as Error).message}`);
            reportedTimeout = true;
          }
          this.#armPresenceRetry(delay);
        }
        this.#reportPresenceInfo();
        if (this.#stopped) break;
        if (this.#notificationError || (!backingOff && this.#changesPending)) continue;
        await new Promise<void>((resolve) => {
          let timer: ReturnType<typeof setTimeout> | undefined;
          const wake = (): void => {
            if (timer !== undefined) clearTimeout(timer);
            this.#wake = undefined;
            resolve();
          };
          this.#wake = wake;
          if (delay !== undefined) timer = setTimeout(wake, Math.min(delay, 2_147_483_647));
        });
      }
    } finally {
      ended = true;
      this.#ready = false;
      this.#watchdogWake?.();
      for (const direction of ["toRemote", "toLocal"] as const) this.#directionWake[direction]?.();
      for (const unsubscribe of unsubscribers) unsubscribe();
      this.#notificationMode = false;
      await this.#watchdogTask?.catch(() => undefined);
    }
  }

  /**
   * Stop the loop without withdrawing either mirror: authority expires by its lease TTL, so an
   * error or process teardown cannot turn incomplete presence into withdrawal evidence. Fence
   * local mirror writes before ending remote stdin, then bound all cleanup by one total deadline.
   */
  stop(): Promise<void> {
    if (this.#stopTask) return this.#stopTask;
    this.#stopTask = this.#stop();
    return this.#stopTask;
  }

  async #stop(): Promise<void> {
    const deadline = Date.now() + (this.options.stopMs ?? DEFAULT_STOP_MS);
    const within = async (work: Promise<unknown>): Promise<void> => {
      const remaining = deadline - Date.now();
      if (remaining <= 0) {
        void work.catch(() => undefined);
        return;
      }
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([work.catch(() => undefined), new Promise<void>((resolve) => {
          timer = setTimeout(resolve, remaining);
          timer.unref?.();
        })]);
      } finally { if (timer !== undefined) clearTimeout(timer); }
    };
    this.#stopped = true;
    this.#ready = false;
    this.#wake?.();
    this.#watchdogWake?.();
    for (const direction of ["toRemote", "toLocal"] as const) this.#directionWake[direction]?.();
    const localFence = this.options.local.fence?.() ?? Promise.resolve();
    const stopError = new Error("Bridge stopped");
    if (this.options.remote.closeGracefully) this.options.remote.closeGracefully(stopError);
    else this.options.remote.close?.(stopError);
    await within(this.#running);
    if (this.#watchdogTask) await within(this.#watchdogTask);
    await within(this.#directions);
    await within(Promise.allSettled([...this.#relayDeliveries]));
    await within(localFence);
    this.#deliveryAge.close();
  }
}

const allBridgedIds = async (side: BridgeSide, after: number): Promise<Set<string>> => {
  const ids = new Set<string>();
  let cursor = after;
  for (;;) {
    const page = await side.bridgedIds(cursor);
    if (!page || !Array.isArray(page.ids) || typeof page.through !== "number") throw new Error("Bridge ids page is invalid");
    for (const id of page.ids) if (typeof id === "string") ids.add(id);
    if (page.done || page.through <= cursor) return ids;
    cursor = page.through;
  }
};

const isPermanent = (error: unknown): boolean =>
  !isIdempotencyKeyReused(error) && !isMeshIncarnationPending(error) && !isMeshSnapshotRetriesExhausted(error) &&
  !isMeshLockTimeout(error) && (error instanceof BridgeOwnershipError ||
    error instanceof Error && /exceeds|not allowed|no bridge stamp|no recipient|no sender|Invalid mesh topic/i.test(error.message));
