import { appendDeliveryOutcome, type DeliverySend } from "../mesh/delivery-outcomes.js";
import { diagnostics } from "../core/diagnostics.js";
import { controlBodyFingerprint, FABRIC_IDEMPOTENCY_KEY_REUSED, FabricIdempotencyKeyReusedError } from "../mesh/control-idempotency.js";
import type { MeshControlIncarnationFence } from "../config.js";
import { FabricBridgeStaleError, FabricParticipantStaleError, mirroredOwnerRoutingStatus, participantLeaseGraceMs, waitForMirroredLeaseRenewal, type FabricMirrorLease, type MirrorLeaseWaitBudget } from "./host-leases.js";
import { retryDelayMs } from "../core/retry-backoff.js";
import { FabricPeerLeaseStaleError } from "../agents/peer-lease-stale.js";
import { assertSteerPriority, type FabricSteerPriority } from "../main-agent.js";
import { copyFabricPrincipal, copyFabricWakeCause, fabricWakeCause, withFabricWakeAdmission, type FabricWakeCause, type FabricPrincipal } from "../fabric-provenance.js";
import { FOLLOW_UP_RUNNING_TASK_MESSAGE, type AgentFollowUpRunningWarning } from "../agents/types.js";
import { FabricTargetTerminalError, terminalRejectionFields } from "../agents/terminal-target.js";
import { createHash, randomUUID } from "node:crypto";
import path from "node:path";
import { mainExecutionCeilingAbortReason, withoutMainExecutionCeiling } from "../async-settlement.js";
import type { FabricActorRunBinding, FabricActorBindingProvenance } from "../actors/types.js";
import { MeshStore, meshOutageAlarmSink, type MeshEvent, type MeshIdentity } from "../mesh/store.js";
import { MeshBackgroundQueue, MeshBackgroundRetry, isMeshWriteDeferred } from "../core/atomic-write.js";
import { rethrowMeshLockTimeout } from "../core/atomic-write.js";
import { MeshConsumptionPausedError, assertMeshConsumption } from "./mesh-consumption.js";
import { isWatchLimitError } from "../core/watch-fallback.js";
import { DegradedObservation } from "../core/observation-watch.js";
import { watchBridgeStore } from "../mesh/change-notifier.js";
import type { FabricMessageSender } from "../protocol.js";
import { eventReceipts } from "./root-inbox-shared.js";
import { FileTransport } from "../transport/file.js";
import { TransportInvalidError, TransportUncertainError, TransportUnavailableError, TransportUnsupportedError, type FabricTransport, type TransportDelivery, type TransportDrainState, type TransportEnvelope, type TransportSent } from "../transport/types.js";
import type { ParticipantRoute, ParticipantView } from "./participant-view.js";
import { assertNatsDrainReceiptForRelease, readNatsDrainReceipt } from "./nats-drain-receipt.js";

const CONTROL_TOPIC = "fabric.control.command";
const ACK_TOPIC = "fabric.control.ack";
const CONTROL_SEEN_PREFIX = "topology/control-seen/";
// The participant directory's host records (topology/participant-directory.ts).
const HOST_PREFIX = "topology/hosts/";
const DEFAULT_POLL_MS = 100;
/** Retained-work retry ceiling: a failed/deferred/gated drain re-arms one-shot timers, never an interval. */
const DRAIN_RETRY_MAX_MS = 30_000;
/** Watcher re-arm: bounded attempts, then a degraded-only minute carrier. */
const WATCH_REARM_BASE_MS = 1_000;
const WATCH_REARM_MAX_MS = 60_000;
const WATCH_REARM_ATTEMPTS = 6;
const DEFAULT_ACK_TIMEOUT_MS = 5_000;
// Contract v2/E11: carrier bounds, not file policy. Composition injects the lane
// setting; this module never reads configuration or widens the frozen transport port.
const DEFAULT_NATS_INBOX_LANES = 8;
const NATS_LANE_CAPACITY = 16;
const NATS_IN_HAND_CAPACITY = 128;
const NATS_WORKING_MS = 10_000;
const NATS_HORIZON_MS = 24 * 60 * 60 * 1_000;
type NatsControlErrorCode = "FABRIC_CONTROL_ID_CONFLICT" | "FABRIC_SENDER_UNVERIFIED";
// Source-local recovery only after an authenticated relay refusal. The UDS relay's
// fixed 4 s receipt window permits at most this 8 s observation window; no idle timer.
// Keep this constant local: importing the optional UDS runtime would widen startup.
const RELAY_ACK_RECOVERY_TTL_MS = 8_000;
const RELAY_ACK_RECOVERY_LIMIT = 256;
const ORIGIN_IDEMPOTENCY_TTL_MS = NATS_HORIZON_MS;
const ORIGIN_IDEMPOTENCY_LIMIT = 100_000;
const UNFENCED_SENDER_WARNING_MS = 60 * 60 * 1_000;
// A cancellation owns a separate retry deadline, longer than a production mesh lock wait.
const CANCELLATION_RETENTION_MS = 60_000;
const CONTROL_COMMAND_EXPIRED = "Fabric control command expired";
const IDEMPOTENCY_KEY_REUSED_ERROR = new FabricIdempotencyKeyReusedError().message;
/** A timed-out message the sender withdrew before its owner claimed it: definitely not delivered. */
export const CONTROL_NOT_DELIVERED = "FABRIC_CONTROL_NOT_DELIVERED";
export const CONTROL_STALE_INCARNATION = "FABRIC_CONTROL_STALE_INCARNATION";
/** Infrastructure refusal, not an owner's ACK or proof that the command did not run. */
export const RELAY_DESTINATION_UNAVAILABLE = "FABRIC_RELAY_DESTINATION_UNAVAILABLE";
export class FabricRelayDestinationUnavailableError extends Error {
  override readonly name = "FabricRelayDestinationUnavailableError";
  readonly code = RELAY_DESTINATION_UNAVAILABLE;
  readonly retryable = true;
  constructor(readonly targetId: string, readonly idempotencyKey: string) {
    super(`Relay destination unavailable for ${targetId}; the outcome is unknown; ` +
      `retry once only with the same idempotencyKey (${idempotencyKey}) and unchanged input.`);
  }
}
export const STALE_INCARNATION_ERROR =
  "Fabric control command targets a previous or unverifiable owner incarnation; re-resolve the participant and retry only if still wanted.";
export class FabricControlStaleIncarnationError extends Error {
  readonly code = CONTROL_STALE_INCARNATION;
  constructor(readonly targetId: string) {
    super(STALE_INCARNATION_ERROR);
    this.name = "FabricControlStaleIncarnationError";
  }
}
/** Pre-publication refusal: an advertised fencing epoch is malformed. */
export class FabricControlIncarnationRequiredError extends Error {
  readonly code = "FABRIC_CONTROL_INCARNATION_REQUIRED";
  constructor(readonly targetId: string) {
    super(`Fabric target ${targetId}: target advertises an invalid owner incarnation; this attempt was not published.`);
    this.name = "FabricControlIncarnationRequiredError";
  }
}

/** Capture only the directory's target epoch; message/data and sender epochs are not authority. */
export const controlOwnerIncarnation = (participant: {
  id: string;
  ownerIncarnation?: string | undefined;
  controlProtocol?: "v1" | "legacy" | undefined;
}): string | undefined => {
  // Absence means an older release, not a failed fencing upgrade. Never fabricate
  // an epoch from payload data, cached sender state, or the protocol label.
  if (participant.ownerIncarnation === undefined) return undefined;
  if (!isIncarnation(participant.ownerIncarnation)) {
    throw new FabricControlIncarnationRequiredError(participant.id);
  }
  return participant.ownerIncarnation;
};
/** One-line counted diagnostic; a legacy route remains authorized by its existing checks. */
export const warnUnfencedControlDelivery = (operation: FabricControlOperation, targetId: string, count: number): void => {
  diagnostics.warn("src/topology/control-plane.ts", `[pi-fabric] Unfenced legacy control delivery count=${count} operation=${operation} target=${JSON.stringify(targetId)}`);
};
const DEFAULT_RESULT_TIMEOUT_MS = 60 * 60 * 1_000;
const MAX_CONTROL_TIMEOUT_MS = 24 * 60 * 60 * 1_000 + 60_000;
// The sender keeps waiting this long past the command deadline. The owner admits a
// command only before its deadline, but its acknowledgement can queue for the mesh lock:
// without this grace a delivered command was reported as timed out and then retried
// (smarty-dev#367). A command not admitted by the deadline is acknowledged as expired.
const MAX_CONTROL_ACK_GRACE_MS = 15_000;
// A bridged request must cover queueing on both hosts and the transport, not just a local poll.
const MIN_BRIDGE_CONTROL_TIMEOUT_MS = 30_000;
// Records other hosts left in the shared state before smarty-dev#643 are checked this often.
const LEGACY_SEEN_CLEANUP_MS = 15 * 60 * 1_000;
// A lock wait that timed out committed nothing, so the step that hit it is retried.
const isLockTimeout = (error: unknown): boolean =>
  isObject(error) && error.code === "FABRIC_MESH_LOCK_TIMEOUT";
// A shared claim for a command with an explicit deadline is deleted this long after it expires,
// once the fleet owner has ended support for runtimes before phase 1 (see #cleanupLegacySeen).
const SHARED_SEEN_GRACE_MS = 10 * 60 * 1_000;
/** Host-reserved policy key; { version: 1, sharedClaims: "expiry" } enables expiry reclamation. */
export const CONTROL_CLAIMS_POLICY_KEY = "topology/control-claims";

// Legacy Main setter wire names remain parseable only so owners can refuse them clearly.
export type FabricControlOperation = "steer" | "followUp" | "stop" | "ask" | "cancel" | "setModel" | "setThinking";

// Keep the reader free to deliver cancellation while asynchronous work waits at its
// commit fence. These commands own their handler and outcome/ACK retries after the cursor.
const detachedControlOperation = (operation: FabricControlOperation): boolean =>
  operation === "ask";

export interface FabricControlCommand {
  /** Hydrated from the mesh envelope, never caller data. */
  eventId?: string;
  /** Hydrated from the admitted MeshEvent envelope, never event.data. */
  principal?: FabricPrincipal | undefined;
  version: 1;
  commandId: string;
  targetId: string;
  operation: FabricControlOperation;
  replyTo: string;
  /** Validated destination link; null binds native delivery, absent is legacy. */
  destinationRemoteHost?: string | null;
  message?: string;
  data?: unknown;
  /** Diagnostic producer metadata, never used for command admission or authority. */
  wakeCause?: FabricWakeCause | undefined;
  triggerTurn?: boolean;
  priority?: FabricSteerPriority;
  binding?: FabricActorRunBinding;
  bindingProvenance?: FabricActorBindingProvenance;
  cancelCommandId?: string;
  /** Target activation captured by the sender before publication waits. */
  ownerIncarnation?: string | undefined;
  /** Root inbox identity of this admitted command event, captured from its mesh envelope. */
  inboxTopic?: string;
  inboxKey?: string;
  inboxReceipts?: string[];
  /** Immutable sender request origin, captured before any publication/lock wait. */
  requestCreatedAt?: number;
  /** Commit time: preserves the existing full delivery/ACK budget after lock waits. */
  requestedAt: number;
  deadlineAt?: number;
  /** The command event's host stamp, copied by the receiving owner; never read from command data. */
  sender?: FabricMessageSender;
}

import type { FabricInterruptErrorCode } from "../interrupt-authority.js";

export interface FabricControlAcceptance {
  code?: "FABRIC_TARGET_TERMINAL";
  finalAnswerReceiptId?: string;
  deliveryStatus?: "undelivered";
  runState?: import("../agents/terminal-target.js").FabricTargetRunState;
  targetId?: string;
  warning?: AgentFollowUpRunningWarning;
  accepted: boolean;
  messageId?: string;
  /** Main requested a new turn at admission; absent on older owners or non-Main targets. */
  triggered?: boolean;
  /** Main held a requested wake; includes the provider retry deadline when applicable. */
  reason?: string;
  /** Main is held by an operator halt; the exact code survives control ACKs. */
  halted?: true;
  /** The owner's followUp queue for a Main target (smarty-dev#1495). */
  pendingFollowUps?: number;
  oldestAgeS?: number;
  /** The owner's Main is idle and its held followUps are stuck (smarty-dev#1826). */
  stalled?: true;
  /** The followUp replaced a held one with the same sender and data.coalesceKey. */
  coalesced?: true;
  replacedMessageId?: string;
  result?: unknown;
  error?: string;
  errorCode?: typeof CONTROL_STALE_INCARNATION | FabricInterruptErrorCode | NatsControlErrorCode;
  /** The owner proves the handler did not run for this command, so a new command cannot deliver twice. */
  notRun?: true;
}

/** An owner's rejection; `notRun` only when the owner proved the handler did not run. */
class FabricControlRejection extends Error {
  /** Marks an owner's remote rejection; the router records no local refusal outcome for it. */
  readonly rejection = "FABRIC_CONTROL_REJECTED";
  constructor(message: string, readonly notRun: boolean, readonly code?: FabricInterruptErrorCode | NatsControlErrorCode) {
    super(message);
    if (code === "FABRIC_INTERRUPT_NOT_AUTHORIZED" || code === "FABRIC_INTERRUPT_RATE_LIMITED") this.name = code === "FABRIC_INTERRUPT_NOT_AUTHORIZED"
      ? "FabricInterruptNotAuthorizedError" : "FabricInterruptRateLimitedError";
  }
}

export interface FabricControlResult {
  warning?: AgentFollowUpRunningWarning;
  queued: true;
  messageId: string;
  routed: "mesh";
  acknowledged: true;
  triggered?: boolean;
  /** Main held a requested wake; includes the provider retry deadline when applicable. */
  reason?: string;
  halted?: true;
  pendingFollowUps?: number;
  oldestAgeS?: number;
  stalled?: true;
  coalesced?: true;
  replacedMessageId?: string;
}

/** A queue count from another owner: a non-negative whole number, or nothing. */
const queueCount = (value: unknown): number | undefined =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : undefined;

const queueDepthOf = (source: Record<string, unknown>): { pendingFollowUps: number; oldestAgeS: number; stalled?: true } | undefined => {
  const pendingFollowUps = queueCount(source.pendingFollowUps);
  const oldestAgeS = queueCount(source.oldestAgeS);
  return pendingFollowUps === undefined || oldestAgeS === undefined
    ? undefined
    : { pendingFollowUps, oldestAgeS, ...(source.stalled === true ? { stalled: true as const } : {}) };
};

/** Keep unknown/legacy receipts unknown; never coerce a malformed report to true. */
const triggeredOf = (source: Record<string, unknown>): { triggered: boolean; reason?: string } | undefined =>
  typeof source.triggered === "boolean" ? {
    triggered: source.triggered,
    ...(source.triggered === false && typeof source.reason === "string" && source.reason.length <= 200
      ? { reason: source.reason } : {}),
  } : undefined;

const haltedOf = (source: Record<string, unknown>): { halted: true; reason: "operator-halt" } | undefined =>
  source.halted === true && source.reason === "operator-halt"
    ? { halted: true, reason: "operator-halt" } : undefined;

/** The owner's coalesce report for a Main followUp (smarty-dev#1495), or nothing. */
const coalescedOf = (source: Record<string, unknown>): { coalesced: true; replacedMessageId: string } | undefined =>
  source.coalesced === true && typeof source.replacedMessageId === "string" && source.replacedMessageId.length <= 200
    ? { coalesced: true, replacedMessageId: source.replacedMessageId }
    : undefined;

/** Copy only the fixed, bounded owner advisory for this exact pending target. */
const runningTaskWarningOf = (source: Record<string, unknown>, targetId: string): { warning: AgentFollowUpRunningWarning } | undefined => {
  const warning = source.warning;
  if (source.accepted !== true || !isObject(warning) ||
    warning.code !== "FABRIC_FOLLOW_UP_RUNNING_TASK" || warning.kind !== "agent" || warning.status !== "running" ||
    typeof warning.targetId !== "string" || !warning.targetId || warning.targetId.length > 200 || warning.targetId !== targetId ||
    warning.message !== FOLLOW_UP_RUNNING_TASK_MESSAGE || warning.message.length > 256) return undefined;
  return { warning: {
    code: "FABRIC_FOLLOW_UP_RUNNING_TASK", targetId: warning.targetId, kind: "agent", status: "running",
    message: FOLLOW_UP_RUNNING_TASK_MESSAGE,
  } };
};

export type FabricControlHandler = (
  command: FabricControlCommand,
  from: MeshIdentity,
  signal: AbortSignal,
  verification?: MeshEvent["verification"],
) => Promise<FabricControlAcceptance> | FabricControlAcceptance;

const controlSeenKey = (hostId: string, commandId: string): string =>
  CONTROL_SEEN_PREFIX +
  createHash("sha256").update(`${hostId}\0${commandId}`).digest("hex");

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isIncarnation = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0 && value.length <= 128;

const controlAcceptanceBytes = (acceptance: FabricControlAcceptance): number =>
  Buffer.byteLength(JSON.stringify(acceptance), "utf8");

const commandFromEvent = (event: MeshEvent): FabricControlCommand | undefined => {
  if (!isObject(event.data) || event.data.version !== 1) return undefined;
  const data = event.data;
  if (
    data.version !== 1 ||
    typeof data.commandId !== "string" ||
    typeof data.targetId !== "string" ||
    (data.operation !== "steer" &&
      data.operation !== "followUp" &&
      data.operation !== "stop" &&
      data.operation !== "ask" &&
      data.operation !== "cancel" &&
      data.operation !== "setModel" &&
      data.operation !== "setThinking") ||
    typeof data.replyTo !== "string" ||
    typeof data.requestedAt !== "number" ||
    (data.requestCreatedAt !== undefined &&
      (typeof data.requestCreatedAt !== "number" || !Number.isFinite(data.requestCreatedAt))) ||
    (data.deadlineAt !== undefined && typeof data.deadlineAt !== "number") ||
    (data.destinationRemoteHost !== undefined && data.destinationRemoteHost !== null &&
      typeof data.destinationRemoteHost !== "string") ||
    (data.operation === "cancel" && typeof data.cancelCommandId !== "string") ||
    (data.ownerIncarnation !== undefined && !isIncarnation(data.ownerIncarnation)) ||
    (data.priority !== undefined && (data.priority !== "interrupt" || data.operation !== "steer")) ||
    (data.bindingProvenance !== undefined &&
      (!isObject(data.bindingProvenance) || data.bindingProvenance.kind !== "owner-defaults" ||
        typeof data.bindingProvenance.rootId !== "string")) ||
    (data.binding !== undefined &&
      (!isObject(data.binding) ||
        (data.binding.model !== undefined && typeof data.binding.model !== "string") ||
        (data.binding.thinking !== undefined && typeof data.binding.thinking !== "string")))
  ) {
    return undefined;
  }
  // Sender diagnostics are never evidence. Derive only from the admitted envelope.
  // A sender stamp is host-set on the event; a payload-supplied one is forged and dropped.
  // Work claims belong in typed nested data, never an unknown outer command key.
  const { sender: _forged, inboxTopic: _topic, inboxKey: _key, inboxReceipts: _receipts, key: _workKey, ...command } = data;
  const wakeCause = data.operation === "steer" || data.operation === "followUp"
    ? fabricWakeCause(event.from, data.operation, event.topic, event.id) : undefined;
  return withFabricWakeAdmission({ ...command, eventId: event.id,
    ...(wakeCause ? {
      inboxTopic: event.topic,
      // Keep authenticated event/dedupe receipts, not raw outer key/ref/id work claims.
      // Explicit nested data remains intact for the native carrier's own work claims.
      inboxReceipts: eventReceipts({ ...event, data: undefined }),
    } : {}),
    wakeCause, principal: event.verification === "mesh" || event.verification === "bridge"
    ? copyFabricPrincipal(event.principal) : undefined, ...(event.sender ? { sender: event.sender } : {}) } as unknown as FabricControlCommand, wakeCause ? [wakeCause] : []);
};

interface FabricControlSeenRecord {
  format: 1;
  hostId: string;
  commandId: string;
  targetId: string;
  expiresAt: number;
  /** Activation that owns the durable claim/outcome, not the process publishing a replay ACK. */
  ownerIncarnation?: string | undefined;
  /** The command carried its own deadlineAt, so every receiver computes the same deadline. */
  explicitDeadline?: boolean;
  /** The command event's sequence, or an upper bound for a record moved from the shared state. */
  sequence?: number;
  acceptance?: FabricControlAcceptance;
  /** NATS claims/outcomes need a covering drain receipt instead of file-log evidence (E17). */
  carrier?: "nats";
  /** NATS-only epoch ms, restamped on each successful CAS write, not on a conflicting retry. */
  writtenAt?: number;
  /** NATS-only immutable wire digest; never computed from hydrated command metadata. */
  bodyDigest?: string;
  /** Authenticated return authority, retained locally with the NATS claim. */
  sourceMachine?: string;
  requesterId?: string;
}

const controlSeenRecord = (value: unknown): FabricControlSeenRecord | undefined => {
  if (!isObject(value) || value.format !== 1) return undefined;
  if (
    typeof value.hostId !== "string" ||
    typeof value.commandId !== "string" ||
    typeof value.targetId !== "string" ||
    typeof value.expiresAt !== "number" ||
    (value.ownerIncarnation !== undefined && !isIncarnation(value.ownerIncarnation))
  ) {
    return undefined;
  }
  return value as unknown as FabricControlSeenRecord;
};

export interface MirroredControlOwner {
  remoteHost: string;
  expiresAt: number;
  mirror?: FabricMirrorLease;
  mirrorLease?: 1;
  /** Validated activation, when supplied by a modern directory. Never retarget after a wait. */
  ownerIncarnation?: string;
  /** Observation exists but does not belong to this owner/link. Never fall back to legacy expiry. */
  bridgeStale?: true;
}

/** Private pre-publication signal only; never an owner ACK and never replay published work. */
class MirrorLeaseAdmissionRequired extends Error {
  constructor(readonly owner: MirroredControlOwner) { super("Mirror renewal required before publication"); }
}

export interface FabricControlPlaneOptions {
  /** Injected carrier; caller owns its lifetime. File remains the default. */
  transport?: FabricTransport;
  /** Exposed for carrier-aware callers; file-mode admission retains its existing gates. */
  participants?: ParticipantView;
  /** NATS keyed-lane concurrency; composition supplies mesh.nats.inboxLanes (default 8, minimum 1). */
  inboxLanes?: number;
  /** Required in NATS mode: reserved fabric-ops[-suffix] credential machine, injected by composition. */
  relayOperatorMachine?: string;
  /** Host-only configured inbox stream, also supplied on FILE rollback; never caller/receipt authority. */
  expectedNatsInboxStream?: string;
  /** Rollback withdrawal hold, epoch ms; composition converts mesh.nats.rollbackHoldUntil (E14-X1). */
  withdrawHoldUntil?: number;
  /** Missing epochs are admitted with sender warnings by default during mixed-release rollout. */
  controlIncarnationFence?: MeshControlIncarnationFence;
  /** Must match this activation's participant directory. */
  ownerIncarnation?: string;
  /** Told each rate-limited unfenced legacy sender warning (default: console.warn). */
  onUnfencedSenderWarning?: (message: string) => void;
  enabled: boolean;
  hostId: string;
  pollMs?: number;
  canConsumeMesh?: () => boolean;
  acknowledgementTimeoutMs?: number;
  /** Native live-owner message window only; no changes to ASK/bridge/explicit deadlines. */
  liveOwnerAckTimeoutMs?: number;
  leaseGraceMs?: number;
  now?: () => number;
  /** Captures validated authority once, then polls only that owner's lease file. */
  captureOwnerLease?: (ownerHostId: string, ownerIdentityId: string, targetId: string) => (() => number) | undefined;
  /** Remote-link command window; at least 30 s, also used for bridged cancellation. */
  bridgeTimeoutMs?: number;
  /** Testable renewal bound, capped at the standard 15 s mirror window. */
  mirrorLeaseWaitMs?: number;
  /** Fresh, directory-validated mirror ownership, including expired leases. */
  readMirroredOwner?: (
    ownerHostId: string,
    ownerIdentityId: string | undefined,
    targetId: string,
  ) => MirroredControlOwner | undefined;
}

export interface FabricControlInput {
  /** Validated participant snapshot; never a model-facing field or inferred at delivery. */
  ownerIncarnation?: string | undefined;
  /** Host-only scope snapshot, not a model-facing parameter. */
  principal?: FabricPrincipal | undefined;
  message?: string;
  data?: unknown;
  wakeCause?: FabricWakeCause | undefined;
  triggerTurn?: boolean;
  priority?: FabricSteerPriority;
  binding?: FabricActorRunBinding;
  bindingProvenance?: FabricActorBindingProvenance;
}

/** Snapshot the actual wire input once; caller mutation cannot change hashed/published bytes. */
const snapshotControlInput = (input: FabricControlInput): FabricControlInput => JSON.parse(JSON.stringify({
  ownerIncarnation: input.ownerIncarnation,
  principal: copyFabricPrincipal(input.principal),
  message: input.message,
  data: input.data,
  wakeCause: copyFabricWakeCause(input.wakeCause),
  triggerTurn: input.triggerTurn,
  priority: input.priority,
  binding: input.binding,
  bindingProvenance: input.bindingProvenance,
})) as FabricControlInput;
const controlRequestFingerprint = (operation: FabricControlOperation, input: FabricControlInput): string =>
  controlBodyFingerprint({
    operation,
    ...(input.message !== undefined ? { message: input.message } : {}),
    ...(input.data !== undefined ? { data: input.data } : {}),
    ...(input.priority !== undefined ? { priority: input.priority } : {}),
    ...(input.triggerTurn !== undefined ? { triggerTurn: input.triggerTurn } : {}),
    ...(input.wakeCause !== undefined ? { wakeCause: input.wakeCause } : {}),
    ...(input.binding !== undefined ? { binding: input.binding } : {}),
    ...(input.bindingProvenance !== undefined ? { bindingProvenance: input.bindingProvenance } : {}),
  }, input.principal);

/** Trust owner-default provenance only from a validated member of that actor's root. */
export const controlActorBindingOptions = (
  command: Pick<FabricControlCommand, "binding" | "bindingProvenance"> & Partial<Pick<FabricControlCommand, "eventId" | "commandId" | "targetId" | "operation">>,
  from: MeshIdentity,
  actorRootId: string | undefined,
  senderRootId: string | undefined,
): { overrides?: FabricActorRunBinding; binding?: FabricActorRunBinding; outcomeSend?: DeliverySend } => {
  const outcomeSend = (command.operation === "steer" || command.operation === "followUp") && command.targetId && command.commandId ? {
    eventId: command.eventId ?? command.commandId, to: command.targetId, from: from.id, mode: command.operation,
  } : undefined;
  const provenance = command.bindingProvenance;
  if (provenance) {
    if (provenance.kind !== "owner-defaults" || !actorRootId || provenance.rootId !== actorRootId ||
      (from.id !== actorRootId && senderRootId !== actorRootId)) {
      throw new Error("Invalid actor owner-default binding provenance");
    }
    return { overrides: command.binding ?? {}, ...(outcomeSend ? { outcomeSend } : {}) };
  }
  // An explicit caller view is fixed, even when one or both fields are absent.
  if (command.binding !== undefined) return { binding: command.binding, ...(outcomeSend ? { outcomeSend } : {}) };
  // Preserve legacy unbound own-root requests, but never promote an empty foreign
  // view into the owner's private session defaults.
  return { ...(actorRootId && (from.id === actorRootId || senderRootId === actorRootId)
    ? {} : { binding: {} }), ...(outcomeSend ? { outcomeSend } : {}) };
};

export interface FabricControlRequestOptions {
  /** Snapshot from the validated participant, never from message/data. */
  routedRemoteHost?: string | null;
  /** Host-only resolution budget shared with the directory/router, not a wire field. */
  mirrorLeaseWaitBudget?: MirrorLeaseWaitBudget | undefined;
  timeoutMs?: number;
  /** Reuse unchanged on a stale/outcome-unknown message retry; scoped to sender/owner/target. */
  idempotencyKey?: string | undefined;
  signal?: AbortSignal;
  /** Host-selected ASK observation policy; only the private branded ceiling qualifies. */
  detachOnMainCeiling?: boolean;
}

interface PendingControlRequest {
  resolve: (acceptance: FabricControlAcceptance) => void;
  reject: (error: Error) => void;
  /** Mirrored messages prearm at admission; other requests arm after publish commits. */
  timer?: NodeJS.Timeout;
  ownerHostId: string;
  ownerIdentityId: string;
  /** NATS-only, captured from ParticipantView once; never a JSON machine label. */
  ownerMachine?: string;
  ownerIncarnation?: string | undefined;
  operation: FabricControlOperation;
  targetId: string;
  commandPublished: boolean;
  readonly requestCreatedAt: number;
  readOwnerExpiry?: () => number;
  readonly idempotencyKey: string;
  /** Canonical semantic wire input; authority/attempt metadata is authenticated separately. */
  readonly bodyFingerprint: string;
  readonly destinationRemoteHost?: string | null;
  /** Original source event, fixed before publication can yield. */
  readonly commandEventId: string;
  /** Validated relay link at admission; never re-resolved from mutable directory state. */
  readonly relayHub?: string;
  cancellationRequested?: boolean;
  cancellationPublished?: boolean;
  mirroredOwner?: MirroredControlOwner;
  signal?: AbortSignal;
  onAbort?: () => void;
}

/** Only captured authority survives a typed refusal: no callbacks, input, lease reads or timers. */
type ControlAcknowledgementAuthority = Readonly<Pick<PendingControlRequest,
  "ownerHostId" | "ownerIdentityId" | "ownerMachine" | "ownerIncarnation" | "targetId" | "destinationRemoteHost">> & {
  readonly mirroredOwner?: Readonly<{ remoteHost: string }>;
};
interface OwnerAcknowledgement {
  readonly from: Readonly<Pick<MeshIdentity, "id">>;
  readonly data: unknown;
}
interface RetainedRelayAcknowledgement {
  readonly commandId: string;
  readonly idempotencyKey: string;
  /** Retain only the digest, never the command body. */
  readonly bodyFingerprint: string;
  readonly expiresAt: number;
  readonly authority: ControlAcknowledgementAuthority;
  readonly acknowledgement?: OwnerAcknowledgement;
}
interface OriginIdempotencyRecord {
  readonly bodyFingerprint: string;
  readonly expiresAt: number;
}

/** Never invoke a getter while capturing a late ACK; wire records must be plain own data. */
const ownDataRecord = (value: unknown): Record<string, unknown> | undefined => {
  if (!isObject(value)) return undefined;
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) return undefined;
  const descriptors = Object.getOwnPropertyDescriptors(value);
  if (Object.values(descriptors).some(descriptor => !Object.hasOwn(descriptor, "value"))) return undefined;
  return Object.fromEntries(Object.entries(descriptors).map(([key, descriptor]) => [key, descriptor.value]));
};
const relayAcknowledgementKey = (commandId: string, idempotencyKey: string): string =>
  JSON.stringify([commandId, idempotencyKey]);

interface ClaimedControlCommand {
  event: MeshEvent;
  claimVersion: number;
  /** A future-skew refusal must survive the shared claim's lock wait. */
  futureSkewed?: boolean;
  acceptance?: FabricControlAcceptance;
  saved?: boolean;
  running?: boolean;
  /** Only saved outcomes enter the serial ACK worker; retain the original wire authority. */
  acknowledgement?: { command: FabricControlCommand; ownerIncarnation: string | null };
}

/** Opaque transport event token, not the logical commandId; never accept an empty/header-unsafe id. */
const isNatsControlEventId = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0 && !/[\s\u0000-\u001f\u007f]/.test(value);

interface NatsControlContext {
  command: FabricControlCommand;
  event: MeshEvent;
  sourceMachine: string;
  deadlineAt: number;
  bodyDigest: string;
  session: AbortController;
}
interface NatsControlFlight {
  context: NatsControlContext;
  started: boolean;
  /** ASK releases its lane at admission, NOT its delivery or duplicate joiners. */
  admitted: Promise<void>;
  admit: () => void;
  completion: Promise<void>;
  resolve: () => void;
  reject: (error: unknown) => void;
  expiryTimer?: NodeJS.Timeout;
}
interface NatsControlLane {
  queue: NatsControlFlight[];
  inHand: number;
  running: boolean;
}
interface NatsControlReceipt {
  delivery: TransportDelivery<TransportEnvelope>;
  session: AbortController;
  workingUntil: number;
  lane?: NatsControlLane;
}
interface NatsRetainedOutcome {
  record: FabricControlSeenRecord;
  version: number;
}

/** Persisted replay owns only an ACK, never claim/save authority or handler retries. */
type SavedControlAcknowledgement = Pick<ClaimedControlCommand, "event" | "running"> & {
  acceptance: FabricControlAcceptance;
  acknowledgement: NonNullable<ClaimedControlCommand["acknowledgement"]>;
};
type OwnedControlCommand = ClaimedControlCommand | SavedControlAcknowledgement;

export class FabricControlPlane {
  readonly incarnation: string;
  readonly #pending = new Map<string, PendingControlRequest>();
  // Insertion order bounds sender-local key/fingerprint bindings; retain no request body.
  readonly #originIdempotency = new Map<string, OriginIdempotencyRecord>();
  // Insertion order is LRU, shared by the authority-only and validated-ACK entries.
  readonly #relayAcknowledgements = new Map<string, RetainedRelayAcknowledgement>();

  #bindOriginIdempotencyKey(commandId: string, idempotencyKey: string, bodyFingerprint: string): void {
    const now = Date.now();
    // Entries are inserted with one fixed TTL and never promoted, so expired records form a prefix.
    for (const [key, entry] of this.#originIdempotency) {
      if (entry.expiresAt > now) break;
      this.#originIdempotency.delete(key);
    }
    // commandId already hashes the scoped caller key, avoiding retention of the raw key.
    const key = commandId;
    const existing = this.#originIdempotency.get(key);
    if (existing) {
      if (existing.bodyFingerprint !== bodyFingerprint) throw new FabricIdempotencyKeyReusedError(idempotencyKey);
      return;
    }
    if (this.#originIdempotency.size >= ORIGIN_IDEMPOTENCY_LIMIT) {
      throw new Error("Fabric origin idempotency window is full; retry after retained keys expire");
    }
    this.#originIdempotency.set(key, { bodyFingerprint, expiresAt: now + ORIGIN_IDEMPOTENCY_TTL_MS });
  }

  #pruneRelayAcknowledgements(): void {
    const now = Date.now();
    for (const [key, entry] of this.#relayAcknowledgements) {
      if (entry.expiresAt <= now) this.#relayAcknowledgements.delete(key);
    }
  }

  /** Body/identity-free diagnostics; access and event receipt reclaim expired recovery state. */
  relayAcknowledgementRecoverySnapshot(): { retained: number; acknowledged: number; limit: number; ttlMs: number } {
    this.#pruneRelayAcknowledgements();
    return { retained: this.#relayAcknowledgements.size,
      acknowledged: [...this.#relayAcknowledgements.values()].filter(entry => entry.acknowledgement !== undefined).length,
      limit: RELAY_ACK_RECOVERY_LIMIT, ttlMs: RELAY_ACK_RECOVERY_TTL_MS };
  }

  #retainRelayAuthority(commandId: string, pending: PendingControlRequest): void {
    this.#pruneRelayAcknowledgements();
    if (this.#closed || !pending.mirroredOwner || typeof pending.destinationRemoteHost !== "string" ||
        pending.relayHub === undefined || (pending.operation !== "steer" && pending.operation !== "followUp")) return;
    const key = relayAcknowledgementKey(commandId, pending.idempotencyKey);
    const authority: ControlAcknowledgementAuthority = Object.freeze({
      ownerHostId: pending.ownerHostId, ownerIdentityId: pending.ownerIdentityId,
      ownerIncarnation: pending.ownerIncarnation, targetId: pending.targetId,
      destinationRemoteHost: pending.destinationRemoteHost,
      mirroredOwner: Object.freeze({ remoteHost: pending.mirroredOwner.remoteHost }),
    });
    // Another refusal cannot renew an existing recovery window or overwrite its original authority.
    const entry = this.#relayAcknowledgements.get(key) ?? Object.freeze({ commandId,
      idempotencyKey: pending.idempotencyKey, bodyFingerprint: pending.bodyFingerprint,
      expiresAt: Date.now() + RELAY_ACK_RECOVERY_TTL_MS, authority });
    this.#relayAcknowledgements.delete(key);
    this.#relayAcknowledgements.set(key, entry);
    while (this.#relayAcknowledgements.size > RELAY_ACK_RECOVERY_LIMIT) {
      this.#relayAcknowledgements.delete(this.#relayAcknowledgements.keys().next().value!);
    }
  }

  #cachedRelayAcknowledgement(commandId: string, pending: ControlAcknowledgementAuthority &
    Pick<PendingControlRequest, "idempotencyKey" | "bodyFingerprint">): OwnerAcknowledgement | undefined {
    this.#pruneRelayAcknowledgements();
    const key = relayAcknowledgementKey(commandId, pending.idempotencyKey);
    const entry = this.#relayAcknowledgements.get(key);
    if (!entry) return undefined;
    // A changed owner/epoch/route invalidates recovery, not the newly resolved request.
    // Check this even before a late ACK arrives; an authority-only entry binds its body too.
    const authority = entry.authority;
    if (!pending.mirroredOwner || typeof pending.destinationRemoteHost !== "string" ||
        authority.ownerHostId !== pending.ownerHostId || authority.ownerIdentityId !== pending.ownerIdentityId ||
        authority.ownerIncarnation !== pending.ownerIncarnation || authority.targetId !== pending.targetId ||
        authority.destinationRemoteHost !== pending.destinationRemoteHost ||
        authority.mirroredOwner?.remoteHost !== pending.mirroredOwner.remoteHost) {
      this.#relayAcknowledgements.delete(key);
      return undefined;
    }
    if (entry.bodyFingerprint !== pending.bodyFingerprint) {
      throw new FabricIdempotencyKeyReusedError(pending.idempotencyKey);
    }
    if (!entry.acknowledgement) return undefined;
    const data = this.#authenticatedAcknowledgement(entry.acknowledgement, pending);
    // A retry must still be on the captured remote link/epoch, not a now-native or new owner.
    if (!pending.mirroredOwner || typeof pending.destinationRemoteHost !== "string" ||
        !data || (pending.ownerIncarnation !== undefined && data.ownerIncarnation !== pending.ownerIncarnation)) {
      this.#relayAcknowledgements.delete(key);
      return undefined;
    }
    this.#relayAcknowledgements.delete(key);
    this.#relayAcknowledgements.set(key, entry); // access promotes LRU, never extends TTL
    return entry.acknowledgement;
  }
  readonly #activeCommands = new Map<
    string,
    { controller: AbortController; requesterId: string; targetId: string }
  >();
  readonly #activeHandlers = new Set<Promise<void>>();
  // Retain the shared claim and exact private outcome across lock timeout AND lease
  // loss until its sequence and ACK commit, never replaying a completed handler.
  readonly #ownedCommands = new Map<string, OwnedControlCommand>();
  #ackWorker: Promise<void> | undefined;
  #ackPublicationFailed = false;
  // One body-free example, not a history: permanent uncertainty outlives dropped worker custody.
  #uncertainAcknowledgement: { commandId: string; outcomeKey: string } | undefined;
  #ackCapacityBlocked = false;

  // Recheck under the actual store lock: checking only before an await is not admission.
  #assertAdmission(): void {
    if (this.#paused) {
      const error = new MeshConsumptionPausedError();
      error.message = "Control release admission is held";
      throw error; // transient hold: never turn this into a refusal or ACK
    }
    assertMeshConsumption(this.options.canConsumeMesh);
  }

  async #putFenced(store: MeshStore, input: Parameters<MeshStore["put"]>[0], admission = false): Promise<{ version: number }> {
    // Preserve the native put path for owners without a lease-consumption fence.
    // Claim custody and handler admission are still rechecked after the await.
    if (!this.options.canConsumeMesh) {
      if (admission) this.#assertAdmission();
      return store.put(input);
    }
    const results = await store.writeBatch({ identity: this.identity, ops: [{ kind: "put", ...input }], beforeCommit: () => {
      if (admission) this.#assertAdmission();
      else assertMeshConsumption(this.options.canConsumeMesh);
    } });
    return { version: results[0]!.version };
  }
  readonly #pollMs: number;
  readonly #ackTimeoutMs: number;
  readonly #bridgeTimeoutMs: number;
  readonly #inboxLanes: number;
  readonly #transport: FabricTransport;
  #stopVerifiedReconnect: (() => void) | undefined;
  readonly #relayOperatorMachine: string | undefined;
  readonly #expectedNatsInboxStream: string | undefined;
  // File-only access stays behind the legacy branches, including reader/cursor semantics.
  get #fileTransport(): FileTransport {
    if (!(this.#transport instanceof FileTransport)) throw new TransportUnsupportedError("Not a file control carrier");
    return this.#transport;
  }
  readonly #natsFlights = new Map<string, NatsControlFlight>();
  readonly #natsLanes = new Map<string, NatsControlLane>();
  readonly #natsReceipts = new Set<NatsControlReceipt>();
  readonly #natsWork = new Set<Promise<void>>();
  readonly #natsPublishes = new Set<Promise<unknown>>();
  readonly #natsOutcomes = new Map<string, NatsRetainedOutcome>();
  readonly #natsCapacityWaiters = new Set<() => void>();
  #natsActiveLanes = 0;
  #natsSession: AbortController | undefined;
  #natsInbox: Promise<void> | undefined;
  #natsInboxRetryTimer: NodeJS.Timeout | undefined;
  #natsInboxRetryDelayMs = 1_000;
  #natsWorkingTimer: NodeJS.Timeout | undefined;
  #natsCleanupTimer: NodeJS.Timeout | undefined;
  #natsCleanupAt = Infinity;
  #natsOutcomeRetryTimer: NodeJS.Timeout | undefined;
  #natsReleaseDrain: TransportDrainState | undefined;
  #lastSequence: number;
  // Drains run at start, on mesh event-log notifications and after this process's own
  // publishes. Idle has no timer: #retryTimer exists only for retained work (a failed or
  // deferred drain's backoff, a gated drain, a retained owned outcome, or shared-state
  // cleanup), #rearmTimer only for a failed watcher's bounded re-arm. The shared
  // observation carrier owns the sole degraded-only minute scan when re-arms exhaust.
  #started = false;
  #stopWatch: (() => void) | undefined;
  #stopStateChanges: (() => void) | undefined;
  #rearmTimer: NodeJS.Timeout | undefined;
  #rearmAttempts = 0;
  #watchWarned = false;
  readonly #observation = new DegradedObservation("control-plane", () => this.#wake(), () => {
    this.#armWatch();
    if (!this.#stopWatch) return false;
    this.#rearmAttempts = 0;
    return true;
  });
  observationHealth(): string[] { return this.#observation.health ? [this.#observation.health] : []; }
  #retryTimer: NodeJS.Timeout | undefined;
  #retryDelay = 0;
  /** A wake no completed drain has served yet; a wake during a drain makes the loop run again. */
  #dirty = false;
  #draining = false;
  #leaseWatchdog: NodeJS.Timeout | undefined;
  #polling: Promise<void> | undefined;
  readonly #backgroundPoll = new MeshBackgroundRetry("control claim/ack poll")
    .alarmsTo(meshOutageAlarmSink(() => ({ mesh: this.mesh, from: this.identity })));
  // Cancellation may become publishable after close, when an admitted command finally commits.
  // Its queue owns that final obligation until success/deadline; idle has no timer or resources.
  readonly #backgroundCancellations = new MeshBackgroundQueue("control cancellation");
  // A proven-notRun request remains close-owned while waiting to resend.
  readonly #resendWaits = new Set<() => void>();
  #closed = false;
  #legacyDeliveryCount = 0;
  readonly #unfencedSenders = new Map<string, { count: number; warnedAt: number }>();
  #paused = false;
  #releaseEpoch = 0;
  #releaseCheckpoint: { epoch: number; state: string } | undefined;
  #releasePublicationFailed = false;
  #handler: FabricControlHandler | undefined;
  #seenCleanupAt = 0;
  #legacySeenCleanupAt = Date.now();
  #legacySeenPending = false;
  /**
   * This host's dedupe records with their outcomes. Only this host reads them, so they live in
   * its own store under the mesh root rather than the shared state that every runtime parses
   * and every heartbeat rewrites under the one lock (smarty-dev#643). The host id is stable
   * across reloads. For file delivery the shared create-only CAS owns admission; the first
   * private save creates the outcome instead of taking another claim (see #acceptCommand).
   */
  readonly #seen: MeshStore;

  constructor(
    readonly mesh: MeshStore,
    readonly identity: MeshIdentity,
    readonly options: FabricControlPlaneOptions,
  ) {
    this.incarnation = options.ownerIncarnation ?? randomUUID();
    if (!isIncarnation(this.incarnation)) throw new Error("Invalid Fabric owner incarnation");
    this.#pollMs = Math.max(20, options.pollMs ?? DEFAULT_POLL_MS);
    this.#ackTimeoutMs = Math.max(this.#pollMs * 4, options.acknowledgementTimeoutMs ?? DEFAULT_ACK_TIMEOUT_MS);
    this.#bridgeTimeoutMs = Math.max(MIN_BRIDGE_CONTROL_TIMEOUT_MS,
      Math.min(MAX_CONTROL_TIMEOUT_MS, Math.floor(options.bridgeTimeoutMs ?? MIN_BRIDGE_CONTROL_TIMEOUT_MS)));
    this.#inboxLanes = Number.isFinite(options.inboxLanes ?? DEFAULT_NATS_INBOX_LANES)
      ? Math.max(1, Math.min(NATS_IN_HAND_CAPACITY, Math.floor(options.inboxLanes ?? DEFAULT_NATS_INBOX_LANES)))
      : DEFAULT_NATS_INBOX_LANES;
    this.#seen = new MeshStore(
      path.join(mesh.root, "control-seen", createHash("sha256").update(options.hostId).digest("hex").slice(0, 32)),
      mesh.maxEventBytes,
      mesh.maxReadEvents,
      // control-seen stays on the file backend (smarty-dev#6477 R20).
      { lockProtocol: mesh.lockProtocol, stateBackend: "file" },
    );
    const transport = options.transport ?? new FileTransport(mesh, { machine: "mesh", hostId: options.hostId });
    if (transport.kind === "nats") {
      if (!options.participants || transport.self.hostId !== options.hostId) {
        throw new TransportUnsupportedError("NATS control requires ParticipantView and its own runtime inbox");
      }
      const operatorMachine = options.relayOperatorMachine;
      if (typeof operatorMachine !== "string" || !/^[a-z0-9][a-z0-9-]{0,31}$/.test(operatorMachine) ||
          !/^fabric-ops(-[a-z0-9]+)?$/.test(operatorMachine) ||
          operatorMachine !== operatorMachine.trim() || operatorMachine === transport.self.machine) {
        throw new TransportInvalidError("NATS control requires relayOperatorMachine: a valid reserved fabric-ops operator machine, distinct from this host");
      }
    } else if (!(transport instanceof FileTransport)) {
      throw new TransportUnsupportedError("File control requires FileTransport");
    }
    this.#transport = transport;
    this.#relayOperatorMachine = transport.kind === "nats" ? options.relayOperatorMachine : undefined;
    this.#expectedNatsInboxStream = options.expectedNatsInboxStream ??
      (transport.kind === "nats" ? `FABRIC_IN_${transport.self.machine}` : undefined);
    // The file reader replays the retained log from its current generation. Durable claims
    // recover unclaimed commands and make interrupted outcomes explicit without re-execution.
    this.#lastSequence = transport instanceof FileTransport ? transport.reader.lastSequence : 0;
  }

  get transportKind(): FabricTransport["kind"] { return this.#transport.kind; }
  get participantView(): ParticipantView | undefined { return this.options.participants; }

  start(handler: FabricControlHandler): void {
    this.#handler = handler;
    if (!this.options.enabled || this.#started) return;
    this.#started = true;
    this.#closed = false;
    this.#paused = false;
    if (this.#transport.kind === "nats") {
      this.#startNats();
      return;
    }
    this.#observation.open();
    this.#armWatch();
    this.#subscribeStateChanges();
    this.#wake(); // a microtask: a synchronous pause() right after start() still gates it
  }

  async request(
    ownerHostId: string,
    targetId: string,
    operation: FabricControlOperation,
    input: FabricControlInput = {},
    ownerIdentityId = ownerHostId,
    options: FabricControlRequestOptions = {},
  ): Promise<FabricControlResult> {
    assertSteerPriority(input.priority, operation);
    // Resends retain both the originally resolved activation and logical request origin.
    const requestCreatedAt = Date.now();
    // Clone semantic JSON plus normalized attribution before publication or any retry yields.
    input = snapshotControlInput(input);
    const natsOwner = this.#transport.kind === "nats"
      ? this.#captureNatsOwner(ownerHostId, ownerIdentityId, targetId, input.ownerIncarnation) : undefined;
    // One logical message key survives both observation retries and a proven-notRun resend.
    options = { ...options, idempotencyKey: options.idempotencyKey ?? randomUUID(),
      mirrorLeaseWaitBudget: options.mirrorLeaseWaitBudget ?? {} };
    let notRunAttempt = 0;
    // Shared across the bounded retry: first admission may discover a mirror even
    // for an older caller that supplied no routing snapshot.
    const destination = { remoteHost: options.routedRemoteHost };
    const timeoutMs = options.timeoutMs ?? this.#ackTimeoutMs;
    const send = (retryBudgetSpentMs = 0) => this.#requestAcceptance(
      ownerHostId,
      targetId,
      operation,
      input,
      ownerIdentityId,
      options,
      destination,
      true,
      retryBudgetSpentMs,
      notRunAttempt,
      requestCreatedAt,
      natsOwner,
    );
    let sent;
    try {
      sent = await send();
    } catch (error) {
      // ponytail: one retry, for messages only, and only when the owner proves the handler did
      // not run (notRun: no claim and no outcome for the command; see #acceptCommand). The
      // expiry text alone proves nothing: an owner restarted past the deadline, or an older
      // owner, answers it without reading its claim (review/astra F1 on #121). An owner that
      // picks commands up late (a fleet relaunch) expired 4 of 2,802 commands, and a manual
      // retry 6 s later worked (smarty-dev#816). A second expiry means the owner is stuck.
      if (
        (operation !== "steer" && operation !== "followUp") ||
        !(error instanceof FabricControlRejection) ||
        !error.notRun ||
        // A conflict, unverified sender or stale route is not permission to mint a
        // fresh logical command. NATS only retains the proven-expiry resend.
        (this.#transport.kind === "nats" && error.message !== `${CONTROL_COMMAND_EXPIRED}; not delivered, safe to resend.`)
      ) throw error;
      const delayMs = retryDelayMs(0, 100, 1_000, Math.max(0, timeoutMs - this.#pollMs * 4));
      await this.#waitForResend(delayMs);
      // The delay consumes the pre-existing second-attempt budget, not a new one.
      // Attempt 1 has a stable sub-key, and is reachable only after proof attempt 0
      // never ran. An external same-key retry re-observes 0, then the same 1 claim.
      notRunAttempt = 1;
      sent = await send(delayMs);
    }
    const { commandId, acceptance } = sent;
    return {
      queued: true,
      messageId: acceptance.messageId ?? commandId,
      routed: "mesh",
      acknowledged: true,
      ...queueDepthOf(acceptance as unknown as Record<string, unknown>),
      ...coalescedOf(acceptance as unknown as Record<string, unknown>),
      ...runningTaskWarningOf(acceptance as unknown as Record<string, unknown>, targetId),
      ...triggeredOf(acceptance as unknown as Record<string, unknown>),
      ...haltedOf(acceptance as unknown as Record<string, unknown>),
    };
  }

  #waitForResend(delayMs: number): Promise<void> {
    if (this.#closed) return Promise.reject(new Error("Fabric control plane closed"));
    return new Promise<void>((resolve, reject) => {
      const cancel = (): void => {
        clearTimeout(timer);
        this.#resendWaits.delete(cancel);
        reject(new Error("Fabric control plane closed"));
      };
      const timer = setTimeout(() => {
        this.#resendWaits.delete(cancel);
        resolve();
      }, delayMs);
      this.#resendWaits.add(cancel);
    });
  }

  async requestResult<T>(
    ownerHostId: string,
    targetId: string,
    operation: FabricControlOperation,
    input: FabricControlInput = {},
    ownerIdentityId = ownerHostId,
    options: FabricControlRequestOptions = {},
  ): Promise<T> {
    const { acceptance } = await this.#requestAcceptance(
      ownerHostId,
      targetId,
      operation,
      snapshotControlInput(input),
      ownerIdentityId,
      { ...options, timeoutMs: options.timeoutMs ?? DEFAULT_RESULT_TIMEOUT_MS },
    );
    if (!Object.prototype.hasOwnProperty.call(acceptance, "result")) {
      throw new Error(`Remote Fabric owner returned no result for ${targetId}`);
    }
    return acceptance.result as T;
  }

  async #requestAcceptance(
    ownerHostId: string,
    targetId: string,
    operation: FabricControlOperation,
    input: FabricControlInput,
    ownerIdentityId: string,
    options: FabricControlRequestOptions,
    destination = { remoteHost: options.routedRemoteHost },
    messageRequest = false,
    retryBudgetSpentMs = 0,
    notRunAttempt = 0,
    requestCreatedAt = Date.now(),
    natsOwner?: Readonly<ParticipantRoute>,
  ): Promise<{ commandId: string; acceptance: FabricControlAcceptance }> {
    if (this.#transport.kind === "nats") {
      return this.#requestNatsAcceptance(ownerHostId, targetId, operation, input, ownerIdentityId,
        options, retryBudgetSpentMs, notRunAttempt, requestCreatedAt, natsOwner);
    }
    if (this.#closed) throw new Error("Fabric control plane closed");
    if (!this.options.enabled) {
      throw new Error("Fabric mesh is disabled; cannot control a remote participant");
    }
    if (!ownerHostId.trim()) throw new Error("Remote participant has no execution owner");
    if (input.ownerIncarnation !== undefined && !isIncarnation(input.ownerIncarnation)) {
      throw new Error("Invalid Fabric owner incarnation");
    }
    // Freeze the resolved activation AND request origin before any publication/lock wait.
    // requestedAt remains commit-stamped and cannot prove a legacy request's origin.
    const ownerIncarnation = input.ownerIncarnation;
    options.signal?.throwIfAborted();
    const idempotencyKey = options.idempotencyKey ?? randomUUID();
    let mirroredOwner: PendingControlRequest["mirroredOwner"];
    const unavailable = (host: string): Error => new Error(
      `Fabric mesh bridge routing to remote host ${host} is unavailable for ${targetId}; ` +
        "the routed owner could not be revalidated; this attempt was not published.",
    );
    try {
      mirroredOwner = this.options.readMirroredOwner?.(ownerHostId, ownerIdentityId, targetId);
    } catch (error) {
      if (typeof destination.remoteHost === "string") throw unavailable(destination.remoteHost);
      throw error;
    }
    if (typeof destination.remoteHost === "string" && mirroredOwner?.remoteHost !== destination.remoteHost) {
      throw unavailable(destination.remoteHost);
    }
    if (destination.remoteHost === null && mirroredOwner) {
      throw new Error(`Fabric native routing is unavailable for ${targetId}; the routed owner changed; this attempt was not published.`);
    }
    if (destination.remoteHost === undefined) {
      destination.remoteHost = mirroredOwner?.remoteHost ?? (this.options.readMirroredOwner ? null : undefined);
    }
    const destinationRemoteHost = destination.remoteHost;
    // All input references belong to the private JSON snapshot. Publish this same body,
    // not a later read of caller-owned objects, after computing the shared wire fingerprint.
    const commandBody = Object.freeze({
      targetId, operation,
      ...(ownerIncarnation !== undefined ? { ownerIncarnation } : {}),
      ...(destinationRemoteHost !== undefined ? { destinationRemoteHost } : {}),
      ...(input.message !== undefined ? { message: input.message } : {}),
      ...(input.data !== undefined ? { data: input.data } : {}),
      ...(input.wakeCause !== undefined ? { wakeCause: input.wakeCause } : {}),
      ...(input.triggerTurn !== undefined ? { triggerTurn: input.triggerTurn } : {}),
      ...(input.priority !== undefined ? { priority: input.priority } : {}),
      ...(input.binding !== undefined ? { binding: input.binding } : {}),
      ...(input.bindingProvenance !== undefined ? { bindingProvenance: input.bindingProvenance } : {}),
    });
    const bodyFingerprint = controlBodyFingerprint(commandBody, input.principal);
    const originFingerprint = controlRequestFingerprint(operation, input);
    const commandId = createHash("sha256")
      .update(JSON.stringify([this.options.hostId, ownerHostId, ownerIdentityId, targetId, operation, idempotencyKey, notRunAttempt]))
      .digest("hex");
    // Bind this caller key to its semantic request before any publication can occur.
    this.#bindOriginIdempotencyKey(commandId, idempotencyKey, originFingerprint);
    const existingPending = this.#pending.get(commandId);
    if (existingPending) {
      if (existingPending.ownerIncarnation === ownerIncarnation &&
          existingPending.destinationRemoteHost === destinationRemoteHost &&
          existingPending.bodyFingerprint !== bodyFingerprint) throw new FabricIdempotencyKeyReusedError(idempotencyKey);
      throw new Error(`Fabric message with idempotencyKey ${idempotencyKey} is already awaiting acknowledgement`);
    }
    // Only directory-validated remote ownership can authorize an infrastructure refusal.
    // Read the canonical host row freshly; never infer a hub from caller data or ACK data.
    let relayHub: string | undefined;
    if (mirroredOwner && destinationRemoteHost === mirroredOwner.remoteHost) {
      const entry = this.mesh.get(HOST_PREFIX + createHash("sha256").update(ownerHostId).digest("hex"), { fresh: true });
      const host = entry?.value;
      if (isObject(host) && host.format === 1 && host.id === ownerHostId && host.rootId === targetId &&
          isObject(host.identity) && host.identity.id === ownerIdentityId && entry?.updatedBy.id === ownerIdentityId &&
          host.remoteHost === mirroredOwner.remoteHost && typeof host.bridgeVia === "string" &&
          /^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$/.test(host.bridgeVia) && host.bridgeVia !== host.remoteHost) {
        relayHub = host.bridgeVia;
      }
    }
    // A refusal's stamp must name this exact publication, not merely the logical retry key.
    const commandEventId = randomUUID();
    const waitBudget = options.mirrorLeaseWaitBudget ?? {};
    const rootGone = (host: string): Error => new Error(
      `Unknown Fabric participant: ${targetId} (its lease mirrored from remote host ${host} lapsed; ` +
        "a fresh bridge sample proves the source lease lapsed); this attempt was not published.");
    const capturedMirror = mirroredOwner;
    const readMirror = (): MirroredControlOwner | undefined => {
      const current = this.options.readMirroredOwner?.(ownerHostId, ownerIdentityId, targetId);
      if (!current || current.remoteHost !== destinationRemoteHost) return undefined;
      if (current.ownerIncarnation !== capturedMirror?.ownerIncarnation ||
        ownerIncarnation !== undefined && current.ownerIncarnation !== undefined && current.ownerIncarnation !== ownerIncarnation) {
        throw new FabricControlStaleIncarnationError(targetId);
      }
      return current;
    };
    const renewMirror = async (initial: MirroredControlOwner): Promise<MirroredControlOwner> => {
      await waitForMirroredLeaseRenewal(this.mesh.root, targetId, initial, readMirror,
        current => current.remoteHost === initial.remoteHost,
        { signal: options.signal, now: this.options.now, waitMs: this.options.mirrorLeaseWaitMs, budget: waitBudget });
      // Every awaited admission rechecks lifecycle, duplicate key and the original authority.
      options.signal?.throwIfAborted();
      if (this.#closed) throw new Error("Fabric control plane closed");
      if (this.#pending.has(commandId)) throw new Error(`Fabric message with idempotencyKey ${idempotencyKey} is already awaiting acknowledgement`);
      const current = readMirror();
      if (!current) throw unavailable(initial.remoteHost);
      const status = mirroredOwnerRoutingStatus(current, (this.options.now ?? Date.now)());
      if (status === "lease-stale") throw rootGone(initial.remoteHost);
      if (status === "bridge-stale") throw new FabricPeerLeaseStaleError(targetId, initial.remoteHost,
        Math.max(0, (this.options.now ?? Date.now)() - current.expiresAt));
      return current;
    };
    // A validated receipt needs no publication or renewed lease. Authenticate it against
    // this request's owner/epoch/link first; cache misses retain the ordinary admission below.
    const cachedAtAdmission = this.#cachedRelayAcknowledgement(commandId, {
      ownerHostId, ownerIdentityId, ownerIncarnation, targetId, idempotencyKey, bodyFingerprint,
      ...(destinationRemoteHost !== undefined ? { destinationRemoteHost } : {}),
      ...(mirroredOwner ? { mirroredOwner } : {}),
    });
    if (cachedAtAdmission && mirroredOwner && !readMirror()) throw unavailable(mirroredOwner.remoteHost);
    if (mirroredOwner && !cachedAtAdmission) {
      const status = mirroredOwnerRoutingStatus(mirroredOwner, (this.options.now ?? Date.now)());
      if (status === "lease-stale") throw rootGone(mirroredOwner.remoteHost);
      if (status === "bridge-stale") mirroredOwner = await renewMirror(mirroredOwner);
    }
    if (ownerIncarnation === undefined && notRunAttempt === 0) {
      // Once per logical legacy delivery, not once per proven-notRun resend.
      warnUnfencedControlDelivery(operation, targetId, ++this.#legacyDeliveryCount);
    }
    // One budget drives admission and ACK timing. A validated fresh native owner gets
    // a bounded busy-owner window; bridges and explicit caller deadlines retain their policy.
    const readOwnerExpiry = typeof destinationRemoteHost === "string" ? undefined :
      this.options.captureOwnerLease?.(ownerHostId, ownerIdentityId, targetId);
    const now = this.options.now ?? Date.now;
    const ownerExpiry = readOwnerExpiry?.();
    if (messageRequest && ownerExpiry !== undefined && now() - ownerExpiry > participantLeaseGraceMs(this.options.leaseGraceMs)) {
      throw new FabricParticipantStaleError(targetId, now() - ownerExpiry, idempotencyKey);
    }
    const liveOwnerWindow = messageRequest && options.timeoutMs === undefined && ownerExpiry !== undefined && ownerExpiry >= now()
      ? Math.min(MAX_CONTROL_TIMEOUT_MS, Math.max(this.#ackTimeoutMs, this.options.liveOwnerAckTimeoutMs ?? 60_000)) : 0;
    const originalTimeoutMs = Math.max(
      liveOwnerWindow,
      this.#pollMs * 4,
      typeof destinationRemoteHost === "string" ? this.#bridgeTimeoutMs : 0,
      Math.min(MAX_CONTROL_TIMEOUT_MS, Math.floor(options.timeoutMs ?? this.#ackTimeoutMs)),
    );
    // Charge the randomized resend sleep after applying native/bridge floors.
    // In particular, the 30s bridge floor must not erase the spent retry budget.
    const timeoutMs = Math.max(this.#pollMs * 4, originalTimeoutMs - retryBudgetSpentMs);
    const ackGraceMs = Math.min(MAX_CONTROL_ACK_GRACE_MS, 2 * timeoutMs);
    let pendingRequest: PendingControlRequest;
    let skipPublication = false;
    let publishedEvent: MeshEvent | undefined;
    let finalOutcome: { outcome: "failed" | "unknown"; reason: string } | undefined;
    const recordFinal = (): void => {
      if (!publishedEvent || !finalOutcome || (operation !== "steer" && operation !== "followUp")) return;
      appendDeliveryOutcome(this.mesh.root, { eventId: publishedEvent.id, to: targetId, from: this.identity.id, mode: operation },
        finalOutcome.outcome, finalOutcome.reason);
      finalOutcome = undefined;
    };
    const acceptance = new Promise<FabricControlAcceptance>((resolve, reject) => {
      const pending: PendingControlRequest = {
        resolve,
        reject,
        ownerHostId,
        ownerIdentityId,
        ownerIncarnation,
        operation,
        targetId,
        commandPublished: false,
        requestCreatedAt,
        idempotencyKey,
        bodyFingerprint,
        commandEventId,
        ...(relayHub !== undefined ? { relayHub } : {}),
        ...(readOwnerExpiry && messageRequest ? { readOwnerExpiry } : {}),
        ...(destinationRemoteHost !== undefined ? { destinationRemoteHost } : {}),
        ...(mirroredOwner ? { mirroredOwner: { ...mirroredOwner } } : {}),
      };
      pendingRequest = pending;
      this.#pending.set(commandId, pending);
      // Revalidate against this NEW admission's owner/epoch/link before publishing anything.
      // Use the ordinary settlement path, including its single clear/timer/abort cleanup.
      let cached: OwnerAcknowledgement | undefined;
      try {
        cached = cachedAtAdmission ?? this.#cachedRelayAcknowledgement(commandId, pending);
      } catch (error) {
        // No publication, timer, lease watchdog or abort listener on a local conflict.
        skipPublication = true;
        this.#clearPending(commandId);
        reject(error as Error);
        return;
      }
      if (cached) {
        skipPublication = true;
        this.#settleAcknowledgement(commandId, pending, cached.data as Record<string, unknown>);
        return;
      }
      if (pending.mirroredOwner) {
        // Capture validated authority first, then arm before publish can wait on the mesh lock.
        // Renewal or a failed directory read cannot extend this sender-local message budget.
        if (messageRequest && (operation === "steer" || operation === "followUp")) {
          // The finite admission budget includes the bridge window and return-leg ACK grace.
          // It is not refreshed by lease renewal, nor reset by a late publish.
          pending.timer = setTimeout(() => this.#timeoutPending(commandId), timeoutMs + ackGraceMs);
          pending.timer.unref();
        }
        this.#startLeaseWatchdog();
      }
      if (pending.readOwnerExpiry) this.#startLeaseWatchdog();
      if (options.signal) {
        const onAbort = (): void => {
          const cancelled = this.#clearPending(commandId);
          const ceiling = operation === "ask" && options.detachOnMainCeiling
            ? mainExecutionCeilingAbortReason(options.signal) : undefined;
          // The remote owner already owns accepted work. Do not translate Main's
          // observation ceiling into an unbranded activation-cancel wire command.
          if (cancelled && !ceiling) void this.#publishCancellation(commandId, cancelled);
          reject(ceiling ?? new Error(`Remote Fabric request cancelled: ${targetId}`));
        };
        pending.signal = options.signal;
        pending.onAbort = onAbort;
        options.signal.addEventListener("abort", onAbort, { once: true });
        if (options.signal.aborted) onAbort();
      }
    });
    // Abort or timeout can reject while the original command publish still holds the mesh lock.
    // Attach a handler now; awaiting the original promise below still preserves the rejection.
    void acceptance.catch(() => undefined);
    try {
      // Settlement must not wait for the original publish or a best-effort cancellation.
      // The publish still completes once, and then cancels if settlement won while in flight.
      const publishOnce = () => this.#fileTransport.publish({
        id: commandEventId,
        topic: CONTROL_TOPIC,
        kind: operation,
        from: this.identity,
        principal: input.principal,
        signal: operation === "ask" && options.detachOnMainCeiling
          ? withoutMainExecutionCeiling(options.signal) : options.signal,
        to: ownerHostId,
        // Stamped at commit (smarty-dev#816): the owner gets the whole timeout, not what is
        // left after this sender waited for the mesh lock (2-3 s under load).
        data: (committedAt: number): FabricControlCommand => {
          // The event data callback runs under the publication lock, before event append.
          // A still-pending queued publish cannot cross a lost lease, swapped link or activation.
          // Once settlement has cleared #pending, preserve the original uncertain in-flight
          // append and its best-effort cancellation rather than turning it into a preflight refusal.
          if (this.#pending.get(commandId) === pendingRequest!) {
            options.signal?.throwIfAborted();
            if (mirroredOwner) {
              const current = readMirror();
              if (!current) throw unavailable(mirroredOwner.remoteHost);
              const status = mirroredOwnerRoutingStatus(current, (this.options.now ?? Date.now)());
              if (status === "lease-stale") throw rootGone(mirroredOwner.remoteHost);
              if (status === "bridge-stale") {
                if (waitBudget.waited) throw new FabricPeerLeaseStaleError(targetId, current.remoteHost,
                  Math.max(0, (this.options.now ?? Date.now)() - current.expiresAt));
                throw new MirrorLeaseAdmissionRequired(current);
              }
            }
          }
          return {
            version: 1,
            commandId,
            ...commandBody,
            replyTo: this.options.hostId,
            requestCreatedAt,
            requestedAt: committedAt,
            deadlineAt: committedAt + timeoutMs,
          };
        },
      });
      const publishing = skipPublication ? Promise.resolve() : publishOnce().catch(async error => {
        if (!(error instanceof MirrorLeaseAdmissionRequired)) throw error;
        // The failed commit callback appended nothing. Clear the prearmed request before
        // awaiting, then re-admit this same command once under the original wait budget.
        const pending = this.#clearPending(commandId);
        if (!pending) throw error;
        mirroredOwner = await renewMirror(error.owner);
        pending.mirroredOwner = { ...mirroredOwner };
        this.#pending.set(commandId, pending);
        if (pending.signal && pending.onAbort) {
          pending.signal.addEventListener("abort", pending.onAbort, { once: true });
          if (pending.signal.aborted) { pending.onAbort(); pending.signal.throwIfAborted(); }
        }
        // Keep the original admission/ACK bound, rather than restarting it after renewal.
        const remaining = Math.max(0, timeoutMs + ackGraceMs - (Date.now() - requestCreatedAt));
        pending.timer = setTimeout(() => this.#timeoutPending(commandId), remaining);
        pending.timer.unref();
        this.#startLeaseWatchdog();
        return publishOnce();
      }).then((event) => {
        this.#wake(); // own publish: never depend on this process's own watcher
        publishedEvent = event;
        recordFinal();
        pendingRequest!.commandPublished = true;
        // Other requests retain their commit-time ACK window. Never overwrite a mirrored
        // message's admission timer, or revive a request already settled while publishing.
        if (this.#pending.get(commandId) === pendingRequest! && !pendingRequest!.timer) {
          pendingRequest!.timer = setTimeout(
            () => this.#timeoutPending(commandId), timeoutMs + ackGraceMs,
          );
          pendingRequest!.timer.unref();
        }
        if (pendingRequest!.cancellationRequested) {
          void this.#publishCancellation(commandId, pendingRequest!);
        }
      });
      await Promise.race([publishing, acceptance.then(() => undefined)]);
      const acknowledged = await acceptance;
      if (!acknowledged.accepted) {
        const terminal = terminalRejectionFields(acknowledged);
        if (terminal && terminal.targetId === targetId) throw new FabricTargetTerminalError(targetId, terminal.finalAnswerReceiptId, terminal.runState);
        const error = acknowledged.error || "Remote Fabric owner rejected command for " + targetId;
        if (acknowledged.errorCode === CONTROL_STALE_INCARNATION) {
          throw new FabricControlStaleIncarnationError(targetId);
        }
        throw new FabricControlRejection(
          acknowledged.notRun === true && error === CONTROL_COMMAND_EXPIRED
            ? `${error}; not delivered, safe to resend.`
            : error,
          acknowledged.notRun === true,
          acknowledged.errorCode,
        );
      }
      return { commandId, acceptance: acknowledged };
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      // A command withdrawn unclaimed at its deadline (smarty-dev#6729) is definitely not delivered.
      // An owner rejection proves that only with notRun; otherwise its handler may have run.
      if ((error instanceof FabricControlRejection && error.notRun) || (isObject(error) && error.notDelivered === true)) finalOutcome = { outcome: "failed", reason };
      else if (error instanceof FabricControlRejection || reason.includes("outcome is unknown") || error instanceof FabricParticipantStaleError ||
        (options.signal?.aborted && isObject(options.signal.reason) && options.signal.reason.name === "TimeoutError")) {
        finalOutcome = { outcome: "unknown", reason };
      }
      recordFinal();
      const cancelled = this.#clearPending(commandId);
      if (cancelled) void this.#publishCancellation(commandId, cancelled);
      throw error;
    }
  }

  #timeoutPending(commandId: string): void {
    const timedOut = this.#clearPending(commandId);
    if (!timedOut) return;
    if (timedOut.readOwnerExpiry) {
      let late = 0;
      try { late = (this.options.now ?? Date.now)() - timedOut.readOwnerExpiry(); }
      catch { /* A failed observation cannot prove a lapse; retain outcome-unknown settlement. */ }
      if (late > 0) {
        timedOut.reject(new FabricParticipantStaleError(timedOut.targetId, late, timedOut.idempotencyKey));
        return; // Keep the durable command/claim: a same-key retry must observe its outcome.
      }
    }
    void this.#publishCancellation(commandId, timedOut);
    // Neither a live lease nor a missing ACK proves the handler did not run. Never replay.
    const unknown = (): void => {
      const error = new Error(
        (timedOut.mirroredOwner
          ? `Fabric mesh bridge to remote host ${timedOut.mirroredOwner.remoteHost} is not responding for ${timedOut.targetId}; `
          : `Timed out waiting for the remote Fabric owner to acknowledge ${timedOut.targetId}; `) +
          // A blind retry mints a new key and can duplicate an admitted command. Only
          // the authenticated relay-refusal path prescribes an explicit same-key retry.
          "the outcome is unknown and it may still be delivered, so a retry can deliver it twice.",
      );
      if (timedOut.idempotencyKey) Object.assign(error, { idempotencyKey: timedOut.idempotencyKey });
      timedOut.reject(error);
    };
    // A bridged owner's claims live in another mesh: only a native owner can be fenced here.
    if (timedOut.ownerMachine !== undefined || timedOut.mirroredOwner || typeof timedOut.destinationRemoteHost === "string" || !timedOut.commandPublished) {
      unknown();
      return;
    }
    // smarty-dev#6729: an owner that stopped consuming (its lease renewal failing) never claimed
    // the command, and past its deadline it never will: on resume it only answers "expired".
    // Make that definite now instead of "outcome unknown" for a message nobody holds.
    void this.#withdrawUnclaimed(commandId, timedOut).then((withdrawn) => {
      if (!withdrawn) return unknown();
      timedOut.reject(Object.assign(new Error(
        `Timed out waiting for the remote Fabric owner to acknowledge ${timedOut.targetId}; ` +
          "the owner never admitted the message and it is now withdrawn: not delivered, nothing was queued. " +
          "Sending it again is safe.",
      ), { code: CONTROL_NOT_DELIVERED, notDelivered: true }));
    }, unknown);
  }

  /**
   * Win the owner's create-only claim for a timed-out command with a terminal "expired, not run"
   * record. The owner's admission uses the same CAS, so a win proves the message was never
   * and will never be delivered. A lost race means the owner holds it: even an owner expiry
   * saved privately is unknown to this sender until its ACK arrives.
   */
  async #withdrawUnclaimed(commandId: string, pending: PendingControlRequest): Promise<boolean> {
    // E14-X1: a NATS-era local claim cannot be disproved by winning the shared file claim.
    if (this.options.withdrawHoldUntil !== undefined && Date.now() < this.options.withdrawHoldUntil) return false;
    const key = controlSeenKey(pending.ownerHostId, commandId);
    const expired = (record: FabricControlSeenRecord | undefined): boolean =>
      record?.hostId === pending.ownerHostId && record.commandId === commandId && record.targetId === pending.targetId &&
        record.acceptance?.accepted === false && record.acceptance.notRun === true &&
        record.acceptance.error === CONTROL_COMMAND_EXPIRED;
    try {
      await this.mesh.put({
        key,
        value: {
          format: 1,
          hostId: pending.ownerHostId,
          commandId,
          targetId: pending.targetId,
          ownerIncarnation: pending.ownerIncarnation,
          expiresAt: Date.now() + this.#ackTimeoutMs,
          explicitDeadline: true,
          acceptance: { accepted: false, error: CONTROL_COMMAND_EXPIRED, notRun: true },
        } satisfies FabricControlSeenRecord,
        identity: this.identity,
        ifVersion: 0,
      });
      return true;
    } catch (error) {
      if (isLockTimeout(error)) return false;
      // A previous withdrawal or older owner may have left a shared terminal expiry.
      // Current owners save expiry privately; their bare shared claim proves no outcome.
      return expired(controlSeenRecord(this.mesh.get(key, { fresh: true })?.value));
    }
  }

  #clearPending(commandId: string): PendingControlRequest | undefined {
    const pending = this.#pending.get(commandId);
    if (!pending) return undefined;
    clearTimeout(pending.timer);
    if (pending.signal && pending.onAbort) {
      pending.signal.removeEventListener("abort", pending.onAbort);
    }
    this.#pending.delete(commandId);
    if (this.#leaseWatchdog && ![...this.#pending.values()].some((request) => request.mirroredOwner || request.readOwnerExpiry)) {
      clearInterval(this.#leaseWatchdog);
      this.#leaseWatchdog = undefined;
    }
    return pending;
  }

  #startLeaseWatchdog(): void {
    if (this.#leaseWatchdog) return;
    // Independent of the async mesh drain: a locked write must not hide a lease lapse.
    this.#leaseWatchdog = setInterval(() => {
      for (const [commandId, pending] of this.#pending) {
        const captured = pending.mirroredOwner;
        if (!captured) {
          if (!pending.readOwnerExpiry) continue;
          let late: number;
          try { late = (this.options.now ?? Date.now)() - pending.readOwnerExpiry(); }
          catch { continue; } // I/O failure is not proof of owner death; retain the fixed bound.
          if (late <= participantLeaseGraceMs(this.options.leaseGraceMs)) continue;
          const stale = this.#clearPending(commandId);
          if (stale) stale.reject(new FabricParticipantStaleError(pending.targetId, late, pending.idempotencyKey));
          continue;
        }
        let current: PendingControlRequest["mirroredOwner"];
        try {
          current = this.options.readMirroredOwner?.(pending.ownerHostId, pending.ownerIdentityId, pending.targetId);
        } catch {
          // A failed read is not evidence of a lapse. The independent request timer stays armed.
          continue;
        }
        // Never adopt a different link's label. Missing ownership cannot renew this lease.
        if (current?.remoteHost === captured.remoteHost) Object.assign(captured, { ...current, mirror: current.mirror, bridgeStale: current.bridgeStale });
        const status = mirroredOwnerRoutingStatus(captured, (this.options.now ?? Date.now)());
        if (status === "live") continue;
        const lapsed = this.#clearPending(commandId);
        if (!lapsed) continue;
        void this.#publishCancellation(commandId, lapsed);
        lapsed.reject(status === "bridge-stale" ? new FabricBridgeStaleError(captured.remoteHost, pending.targetId) : new Error(
          `Fabric lease observed on remote host ${captured.remoteHost} lapsed for ${pending.targetId}; ` +
            "the bridge observation is fresh and the remote owner's lease is old; " +
            "the outcome is unknown and it may still be delivered, so a retry can deliver it twice.",
        ));
      }
    }, this.#pollMs);
    this.#leaseWatchdog.unref();
  }

  async #publishCancellation(
    commandId: string,
    pending: PendingControlRequest,
  ): Promise<void> {
    if (pending.ownerMachine !== undefined) return this.#publishNatsCancellation(commandId, pending);
    if (!pending.commandPublished) {
      pending.cancellationRequested = true;
      return;
    }
    if (pending.cancellationPublished) return;
    pending.cancellationPublished = true;
    const expiresAt = Date.now() + Math.max(CANCELLATION_RETENTION_MS, 2 * this.#ackTimeoutMs);
    const cancelCommandId = randomUUID();
    const cancellation = {
      topic: CONTROL_TOPIC,
      kind: "cancel",
      from: this.identity,
      to: pending.ownerHostId,
      data: (committedAt: number): FabricControlCommand => ({
        version: 1,
        commandId: cancelCommandId,
        targetId: pending.targetId,
        operation: "cancel",
        cancelCommandId: commandId,
        replyTo: this.options.hostId,
        ...(pending.ownerIncarnation !== undefined ? { ownerIncarnation: pending.ownerIncarnation } : {}),
        ...(pending.destinationRemoteHost !== undefined
          ? { destinationRemoteHost: pending.destinationRemoteHost } : {}),
        requestCreatedAt: pending.requestCreatedAt,
        requestedAt: committedAt,
        deadlineAt: committedAt + Math.max(this.#ackTimeoutMs,
          typeof pending.destinationRemoteHost === "string" ? this.#bridgeTimeoutMs : 0),
      }),
    };
    await this.#backgroundCancellations.enqueue(async () => {
      if (Date.now() > expiresAt) return undefined;
      const event = await this.#fileTransport.publish(cancellation);
      this.#wake();
      return event;
    });
  }

  /** Reload: leave commands queued; the next activation fences execution-bound addresses. */
  pause(_strict: unknown = false): void {
    if (!this.#paused) this.#releaseEpoch++;
    this.#paused = true;
    if (this.#transport.kind === "nats") {
      this.#stopNatsInbox(undefined, typeof _strict === "boolean" ? undefined : _strict);
      clearTimeout(this.#natsInboxRetryTimer);
      this.#natsInboxRetryTimer = undefined;
      clearTimeout(this.#natsCleanupTimer);
      this.#natsCleanupTimer = undefined;
      clearTimeout(this.#natsOutcomeRetryTimer);
      this.#natsOutcomeRetryTimer = undefined;
      return;
    }
    // Pending work stays #dirty; resume() drains it. A paused plane arms no retry.
    clearTimeout(this.#retryTimer);
    this.#retryTimer = undefined;
  }

  resume(): void {
    if (this.#closed || !this.#paused) return;
    this.#paused = false;
    this.#releaseEpoch++;
    if (this.#transport.kind === "nats") { this.#startNats(); return; }
    this.#wake();
  }

  /** Join through outcome and ACK publication, not merely the host admission counter. */
  async checkpointForRelease(valid?: () => boolean): Promise<void> {
    if (this.#transport.kind === "nats") return this.#checkpointNatsForRelease(valid);
    const epoch = this.#releaseEpoch;
    const check = () => {
      if (!this.#paused || epoch !== this.#releaseEpoch || valid?.() === false) throw new Error("Control release checkpoint is stale or not paused");
    };
    check();
    await this.#polling?.catch(error => { if (!(error instanceof MeshConsumptionPausedError)) throw error; });
    check();
    await Promise.all([...this.#activeHandlers]);
    check();
    await this.#backgroundCancellations.checkpointForRelease();
    check();
    if (this.#activeCommands.size || this.#pending.size || this.#resendWaits.size || this.#ownedCommands.size || this.#releasePublicationFailed) {
      throw new Error("Control release has unsettled publication obligations");
    }
    const state = JSON.stringify(this.#readReleaseState());
    check();
    this.#releaseCheckpoint = { epoch, state };
  }

  /** E14-X1/E17: only a host/stream-bound drain proof covering the last NATS write permits a skip. */
  #releaseNeedsFileEvidence(value: unknown): boolean {
    const record = controlSeenRecord(value);
    if (record?.carrier !== "nats") return true; // File-only hosts never read a receipt or require a stream.
    assertNatsDrainReceiptForRelease(readNatsDrainReceipt(this.#seen.root), {
      hostId: this.options.hostId,
      writtenAt: record.writtenAt,
      now: (this.options.now ?? Date.now)(),
      ...(this.#expectedNatsInboxStream !== undefined
        ? { expectedNatsInboxStream: this.#expectedNatsInboxStream } : {}),
    });
    return false;
  }

  #readReleaseState(): unknown {
    if (this.#transport.kind === "nats") {
      // The synchronous receipt API cannot perform a fresh broker RPC. This is the
      // drain snapshot acquired by checkpointForRelease, explicitly scoped as such.
      return { localOutcomes: this.#readNatsReleaseOutcomes(), drain: this.#natsReleaseDrain };
    }
    // E17 supersedes E14's unconditional skip, including for marked records with an outcome.
    const localOutcomes = this.#seen.listAll(CONTROL_SEEN_PREFIX, { fresh: true })
      .filter(entry => this.#releaseNeedsFileEvidence(entry.value)).sort((a, b) => a.key.localeCompare(b.key));
    const sharedClaims = this.mesh.listAll(CONTROL_SEEN_PREFIX, { fresh: true })
      .filter(entry => {
        const record = controlSeenRecord(entry.value);
        return record?.hostId === this.options.hostId && this.#releaseNeedsFileEvidence(record);
      }).sort((a, b) => a.key.localeCompare(b.key));
    const acknowledgements = this.#readFileReleaseAcknowledgements(localOutcomes);
    const outcomes = new Map(localOutcomes.map(entry => [entry.key, controlSeenRecord(entry.value)]));
    for (const entry of sharedClaims) {
      const record = controlSeenRecord(entry.value)!;
      if (!record.acceptance && !outcomes.get(entry.key)?.acceptance) throw new Error("Control shared claim outcome disk readback is unconfirmed");
    }
    return { localOutcomes, sharedClaims, acknowledgements, consumedSequence: this.#lastSequence };
  }

  /** File-era outcomes require the original command and correlated ACK, on either carrier. */
  #readFileReleaseAcknowledgements(localOutcomes: readonly { value: unknown }[]): MeshEvent[] {
    const acknowledgements: MeshEvent[] = [];
    const commands = new Map<number, MeshEvent>();
    let offset = 0, read = 0;
    for (;;) {
      const tail = this.mesh.tail(offset, 100);
      read += tail.events.length;
      if (read > 10_000) throw new Error("Control ACK disk readback exceeds bounded receipt scan; retain A");
      for (const event of tail.events) {
        if (event.topic === ACK_TOPIC && event.from.id === this.identity.id) acknowledgements.push(event);
        if (event.topic === CONTROL_TOPIC && event.to === this.options.hostId) commands.set(event.sequence, event);
      }
      if (tail.events.length < 100 || tail.nextOffset === offset) break;
      offset = tail.nextOffset;
    }
    for (const entry of localOutcomes) {
      const record = controlSeenRecord(entry.value);
      if (!record?.acceptance) throw new Error("Control disk readback has a claim without a confirmed outcome");
      const acceptance = record.acceptance;
      const input = record.sequence === undefined ? undefined : commands.get(record.sequence);
      const command = input && commandFromEvent(input);
      if (!command || command.commandId !== record.commandId || command.targetId !== record.targetId) {
        throw new Error("Control outcome input disk readback is unavailable; retain A");
      }
      const ack = acknowledgements.find(event => event.to === command.replyTo && isObject(event.data) && event.data.commandId === record.commandId &&
        event.data.targetId === record.targetId && event.data.ownerIncarnation === record.ownerIncarnation && event.data.accepted === acceptance.accepted &&
        ["result", "error", "messageId", "notRun"].every(key =>
          JSON.stringify((event.data as Record<string, unknown>)[key]) === JSON.stringify((acceptance as unknown as Record<string, unknown>)[key])));
      if (!ack) throw new Error("Control outcome ACK disk readback is unavailable; retain A");
    }
    return acknowledgements;
  }

  releaseCheckpointReceipt(): unknown {
    const saved = this.#releaseCheckpoint;
    if (!this.#paused || !saved || saved.epoch !== this.#releaseEpoch || this.#polling || this.#activeCommands.size ||
        this.#activeHandlers.size || this.#pending.size || this.#resendWaits.size || this.#ownedCommands.size || this.#releasePublicationFailed) {
      throw new Error("Control release checkpoint receipt is unavailable or unsettled");
    }
    if (this.#transport.kind === "nats" && (this.#natsWork.size || this.#natsPublishes.size || this.#natsReceipts.size ||
        this.#natsFlights.size || this.#natsOutcomes.size || this.#natsReleaseDrain?.pending !== 0 || this.#natsReleaseDrain.ackPending !== 0)) {
      throw new Error("NATS control release checkpoint is unsettled");
    }
    const state = this.#readReleaseState();
    if (JSON.stringify(state) !== saved.state) throw new Error("Control release checkpoint disk readback changed");
    return { admissionHeld: true, state, receiptScope: this.#transport.kind === "nats"
      ? "persisted local outcomes and broker drain snapshot at checkpoint, not a fresh broker read"
      : "persisted claims/outcomes and correlated ACK log, not UI quiet" };
  }

  /** Join in-flight NATS inbox deliveries before a short-lived owner withdraws after handling a reply. */
  async settleNatsInbox(): Promise<void> {
    if (this.#transport.kind !== "nats") return;
    while (this.#natsWork.size) await Promise.allSettled([...this.#natsWork]);
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    if (this.#transport.kind === "nats") return this.#closeNats();
    // Fence new requests before joining polls: a late notRun ACK must not arm a resend.
    this.#closed = true;
    for (const cancel of this.#resendWaits) cancel();
    this.#started = false;
    this.#observation.close();
    this.#stopWatch?.();
    this.#stopWatch = undefined;
    try { this.#stopStateChanges?.(); } catch {}
    this.#stopStateChanges = undefined;
    clearTimeout(this.#rearmTimer);
    this.#rearmTimer = undefined;
    clearTimeout(this.#retryTimer);
    this.#retryTimer = undefined;
    await this.#polling?.catch(() => undefined);
    if (!this.#paused) await this.#drain().catch(() => undefined);
    const cancellations: Promise<void>[] = [];
    for (const id of [...this.#pending.keys()]) {
      const pending = this.#clearPending(id);
      if (!pending) continue;
      cancellations.push(this.#publishCancellation(id, pending));
      pending.resolve({ accepted: false, error: "Fabric control plane closed" });
    }
    await Promise.allSettled(cancellations);
    for (const active of this.#activeCommands.values()) active.controller.abort();
    await Promise.allSettled([...this.#activeHandlers]);
    this.#handler = undefined;
    this.#originIdempotency.clear();
    this.#relayAcknowledgements.clear();
    const pendingAcknowledgements = [...this.#ownedCommands].filter(([, owned]) => owned.acknowledgement);
    if (this.#ackPublicationFailed || pendingAcknowledgements.length) {
      const obligations = [
        ...(this.#uncertainAcknowledgement ? [
          `uncertain ACK commandId=${JSON.stringify(this.#uncertainAcknowledgement.commandId)} savedOutcomeKey=${JSON.stringify(this.#uncertainAcknowledgement.outcomeKey)} (first uncertainty)`,
        ] : []),
        ...pendingAcknowledgements.map(([key, owned]) =>
          `pending ACK commandId=${JSON.stringify(owned.acknowledgement!.command.commandId)} savedOutcomeKey=${JSON.stringify(key)}`),
      ];
      // Close must let Main finish teardown. This is not a release receipt or an ACK retry:
      // keep the durable result and failure flag, without logging private acceptance/message bodies.
      try {
        console.warn(`[pi-fabric] Control close has unsettled acknowledgement obligations: ${obligations.join("; ")}. ` +
          "Saved outcomes were committed; close does not discard them. Next start can re-publish retained commands' saved outcomes without repeating handlers. Checkpoint/release remains refused.");
      } catch { /* A diagnostic sink must not interrupt runtime teardown. */ }
    }
  }

  /** True only when a drain read the log to its end; a gated or paused drain retains its work. */
  async #poll(): Promise<boolean> {
    if (this.#gated()) return false;
    if (this.#polling) { await this.#polling; return false; }
    const operation = this.#drain();
    // Joiners (checkpoint/close) still observe a failure; the drain loop below handles it.
    const polling = operation.then(() => undefined);
    void polling.catch(() => undefined);
    this.#polling = polling;
    try {
      const complete = await operation;
      if (!this.#ownedCommands.size) this.#backgroundPoll.success();
      return complete;
    } catch (error) {
      if (!(error instanceof MeshConsumptionPausedError)) throw error;
      return false;
    } finally {
      if (this.#polling === polling) this.#polling = undefined;
    }
  }

  #gated(): boolean {
    return this.#closed || this.#paused || !this.options.enabled || this.options.canConsumeMesh?.() === false;
  }

  #armWatch(): void {
    if (this.#closed || this.#stopWatch) return;
    try {
      this.#stopWatch = watchBridgeStore(this.mesh.root, (kind) => {
        this.#rearmAttempts = 0; // a delivered notification proves the watch works
        // Commands and ACKs live in the event log (or its rotated generation). Presence or
        // lease writes matter only to work a consumption gate is holding back.
        if (kind === "events" || this.#dirty) this.#wake();
      }, (error) => this.#watchFailed(error));
    } catch (error) {
      this.#watchFailed(error);
    }
  }

  // A replicated state backend (NATS) commits remotely where no local fs watch can see it.
  // Its optional MeshStore change listener is one more dirty wake; on the filesystem
  // backends it is a no-op and the directory watch above remains the notifier.
  #subscribeStateChanges(): void {
    if (this.#stopStateChanges) return;
    try {
      this.#stopStateChanges = this.mesh.subscribeStateChanges(() => this.#wake());
    } catch (error) {
      // Not fatal: the fs watch and own-publish wakes still drain; say so once.
      try { diagnostics.warn("src/topology/control-plane.ts", `[pi-fabric] control plane: mesh state change subscription failed (${error instanceof Error ? error.message : String(error)}); relying on mesh directory notifications.`); } catch {}
    }
  }

  // One rescan plus bounded re-arms; exhaustion retains the shared degraded-only carrier.
  #watchFailed(error: unknown): void {
    this.#stopWatch?.();
    this.#stopWatch = undefined;
    if (this.#closed || this.#observation.degraded) return;
    const delay = Math.min(WATCH_REARM_MAX_MS, WATCH_REARM_BASE_MS * 2 ** this.#rearmAttempts);
    if (!this.#watchWarned) {
      this.#watchWarned = true;
      const cause = isWatchLimitError(error)
        ? "ENOSPC: inotify watch limit, not disk space (raise fs.inotify.max_user_watches)"
        : error instanceof Error ? error.message : String(error);
      try {
        diagnostics.warn("src/topology/control-plane.ts", `[pi-fabric] control plane: mesh notifications failed for ${this.mesh.root} (${cause}); ` +
          `rescanned once, re-arming in ${delay} ms with bounded retries. Normal operation has no polling; ` +
          "exhausted watches scan and re-arm once per minute; a sender's own ACK deadline still settles its request.");
      } catch {}
    }
    this.#wake();
    if (this.#rearmTimer) return;
    if (this.#rearmAttempts >= WATCH_REARM_ATTEMPTS) { this.#observation.enter(); return; }
    this.#rearmAttempts++;
    this.#rearmTimer = setTimeout(() => {
      this.#rearmTimer = undefined;
      if (this.#closed) return;
      this.#armWatch();
      if (this.#stopWatch) this.#wake(); // notifications may have been missed while unarmed
    }, delay);
    this.#rearmTimer.unref();
  }

  #wake(): void {
    if (this.#closed || !this.options.enabled || !this.#started) return;
    this.#dirty = true;
    queueMicrotask(() => {
      void this.#runDrains();
    });
  }

  // Serial drain loop. A wake during a drain sets #dirty, so the loop drains again instead of
  // dropping it. Work a drain could not finish keeps #dirty and arms one bounded one-shot retry.
  async #runDrains(): Promise<void> {
    if (this.#draining) return;
    this.#draining = true;
    try {
      while (this.#dirty && !this.#gated()) {
        clearTimeout(this.#retryTimer);
        this.#retryTimer = undefined;
        this.#dirty = false;
        let complete = false;
        const outcome = await this.#backgroundPoll.run(async () => { complete = await this.#poll(); }, false);
        if (outcome === "done" && this.#ackCapacityBlocked && this.#ackWorker) {
          this.#dirty = true;
          if (this.#acknowledgementsFull()) return; // settlement wakes the reader; capacity owns no timer
          continue;
        }
        if (outcome === "done" && complete) {
          // A detached outcome whose save/ACK failed is retained, not running: retry it with backoff.
          if ([...this.#ownedCommands.values()].some((owned) => !owned.running)) {
            this.#dirty = true;
            this.#scheduleRetry(this.#backgroundPoll.waitMs > 0 ? "retry" : "failed");
            return;
          }
          this.#retryDelay = 0;
          continue;
        }
        this.#dirty = true;
        this.#scheduleRetry(outcome);
        return;
      }
      // A consumption gate held a wake back: it stays #dirty with no timer. The gate reopens
      // through a presence/lease notification (the watcher wakes a dirty plane) or resume().
      if (!this.#dirty) this.#scheduleSeenCleanup();
    } finally {
      this.#draining = false;
    }
  }

  #scheduleRetry(outcome: "done" | "retry" | "failed" | "skipped"): void {
    // A gated plane retains its dirty work for the reopen wake; only a drain that could run retries.
    if (this.#gated()) return;
    let delay: number;
    if (outcome === "retry" || outcome === "skipped") delay = Math.max(1, this.#backgroundPoll.waitMs);
    else {
      this.#retryDelay = Math.min(DRAIN_RETRY_MAX_MS, this.#retryDelay ? this.#retryDelay * 2 : this.#pollMs);
      delay = this.#retryDelay;
    }
    clearTimeout(this.#retryTimer);
    this.#retryTimer = setTimeout(() => {
      this.#retryTimer = undefined;
      this.#wake();
    }, delay);
    this.#retryTimer.unref();
  }

  /** Retained claims keep their expiry and shared-state cleanup duties, not a hot idle poll.
   * Empty state has no deadline; shared records are swept at the existing 15-minute cadence. */
  #scheduleSeenCleanup(): void {
    if (this.#closed || this.#paused || this.#retryTimer) return;
    const legacyDeadline = this.#legacySeenPending
      ? this.#legacySeenCleanupAt + LEGACY_SEEN_CLEANUP_MS : Infinity;
    // An overdue deadline must wait until cleanup can act, not spin through its throttle.
    const expiresAt = Math.max(this.#seenCleanupAt + this.#ackTimeoutMs, legacyDeadline);
    if (!Number.isFinite(expiresAt)) return;
    this.#retryTimer = setTimeout(() => {
      this.#retryTimer = undefined;
      this.#wake();
    }, Math.min(2_147_483_647, Math.max(1, expiresAt - Date.now() + 1)));
    this.#retryTimer.unref();
  }

  /** A detached execution settled while its outcome is still owned (deferred save/ACK): retry
   * it with bounded backoff, without needing another mesh event. */
  #retryRetainedOwned(key: string, owned: OwnedControlCommand): void {
    if (this.#closed || this.#ownedCommands.get(key) !== owned || owned.running) return;
    this.#dirty = true;
    if (this.#draining) return; // the running loop sees the retained outcome when it finishes
    this.#scheduleRetry(this.#backgroundPoll.waitMs > 0 ? "retry" : "failed");
  }

  async #drain(): Promise<boolean> {
    // Partial claims retain custody across PREPARE, including spent deadlines.
    // Their expiry is an outcome decision on resume, never permission to drop ownership.
    this.#pruneRelayAcknowledgements();
    this.#ackCapacityBlocked = false;
    this.#resumeAcknowledgements();
    // ACK-only custody never joins the reader or re-enters the handler/save retry traversal.
    for (const owned of this.#ownedCommands.values()) {
      if ("claimVersion" in owned && !owned.acknowledgement && !owned.running && (!this.#paused || owned.acceptance)) await this.#acceptCommand(owned.event);
    }
    // The reader commits a sequence only once handling succeeds; a failed command
    // leaves its page/offset for the bounded retry (smarty-dev#424). Keep ACKs
    // synchronous: awaiting an ACK callback would yield before its cursor commit.
    const complete = await this.#fileTransport.reader.drain(event => {
      if (event.topic === ACK_TOPIC) {
        // A hub speaks only for relay infrastructure, never for the destination owner.
        const envelope = ownDataRecord(event);
        const data = envelope && ownDataRecord(envelope.data);
        if (data?.errorCode === RELAY_DESTINATION_UNAVAILABLE || data?.errorCode === FABRIC_IDEMPOTENCY_KEY_REUSED) this.#acceptRelayRefusal(event);
        else if (data) this.#acceptAcknowledgement(event);
        return;
      }
      return this.#acceptCommand(event);
    }, () => this.#paused, () => this.options.canConsumeMesh?.() !== false, event => {
      // Release readback needs evidence at the exact cursor commit, not after an await.
      this.#lastSequence = event.sequence;
    }, () => {
      if (this.#acknowledgementsFull()) {
        this.#ackCapacityBlocked = true;
        return false; // stop before fetching the next page; ACK settlement wakes the reader
      }
      return true;
    }, event => {
      if (event.topic === CONTROL_TOPIC && this.#acknowledgementsFull()) {
        const command = commandFromEvent(event);
        if (command && command.operation !== "cancel" && !this.#ownedCommands.has(controlSeenKey(this.options.hostId, command.commandId))) {
          this.#ackCapacityBlocked = true;
          return false; // leave this new command unread; never drop an ACK obligation
        }
      }
      return true;
    });
    if (!complete) return false;
    await this.#cleanupSeen(Date.now()).catch(rethrowMeshLockTimeout);
    return true;
  }

  #acceptRelayRefusal(event: MeshEvent): void {
    const data = event.data;
    if (!ownDataRecord(event) || !ownDataRecord(event.from) || !ownDataRecord(data) ||
        !isObject(data) || typeof data.commandId !== "string" || !ownDataRecord(data.bridge)) return;
    const pending = this.#pending.get(data.commandId);
    if (!pending || pending.relayHub === undefined || !pending.mirroredOwner ||
        Object.hasOwn(event, "text") || Object.hasOwn(event, "message") ||
        Object.keys(data).some((field) => !["version", "commandId", "targetId", "accepted", "error", "errorCode", "retryable", "bridge"].includes(field)) ||
        pending.destinationRemoteHost !== pending.mirroredOwner.remoteHost ||
        event.to !== this.options.hostId || event.verification !== "bridge" ||
        event.from.id !== `bridge:${pending.relayHub}` || event.from.name !== "mesh-bridge" || event.from.kind !== "main" ||
        data.version !== 1 || data.targetId !== pending.targetId || data.accepted !== false ||
        !((data.errorCode === RELAY_DESTINATION_UNAVAILABLE && data.error === "Relay destination unavailable" && data.retryable === true) ||
          (data.errorCode === FABRIC_IDEMPOTENCY_KEY_REUSED && data.error === IDEMPOTENCY_KEY_REUSED_ERROR && data.retryable === false)) ||
        !isObject(data.bridge) ||
        Object.keys(data.bridge).some((field) => field !== "from" && field !== "id") ||
        data.bridge.from !== pending.relayHub || data.bridge.id !== pending.commandEventId ||
        // Infrastructure cannot attest an owner epoch or a definite non-delivery outcome.
        ["ownerIncarnation", "notRun", "notDelivered"].some((field) => Object.hasOwn(data, field))) return;
    if (data.errorCode === RELAY_DESTINATION_UNAVAILABLE) this.#retainRelayAuthority(data.commandId, pending);
    this.#clearPending(data.commandId);
    // Reject directly with our own pending key; a conflict retains no authority and cannot retry.
    // Do not broaden owner ACK error codes or enter the notRun resend path.
    pending.reject(data.errorCode === FABRIC_IDEMPOTENCY_KEY_REUSED
      ? new FabricIdempotencyKeyReusedError(pending.idempotencyKey)
      : new FabricRelayDestinationUnavailableError(pending.targetId, pending.idempotencyKey));
  }

  #acceptAcknowledgement(event: MeshEvent): void {
    this.#pruneRelayAcknowledgements();
    const envelope = ownDataRecord(event);
    const data = envelope && ownDataRecord(envelope.data);
    const from = envelope && ownDataRecord(envelope.from);
    if (!data || !from || typeof data.commandId !== "string" || typeof from.id !== "string") return;
    const pending = this.#pending.get(data.commandId);
    if (pending) {
      if (this.#authenticatedAcknowledgement({ from: { id: from.id }, data }, pending)) this.#settleAcknowledgement(data.commandId, pending, data);
      return;
    }
    // No general ACK cache: only an exact command with previously retained refusal authority.
    for (const [key, entry] of this.#relayAcknowledgements) {
      if (entry.commandId !== data.commandId || envelope?.verification !== "bridge" ||
          envelope.to !== this.options.hostId || typeof data.accepted !== "boolean" ||
          data.errorCode === RELAY_DESTINATION_UNAVAILABLE || data.errorCode === FABRIC_IDEMPOTENCY_KEY_REUSED ||
          (entry.authority.ownerIncarnation !== undefined && data.ownerIncarnation !== entry.authority.ownerIncarnation)) continue;
      const bridge = ownDataRecord(data.bridge);
      const warning = data.warning === undefined ? undefined : ownDataRecord(data.warning);
      if (!bridge || (data.warning !== undefined && !warning)) continue;
      const safeData = { ...data, bridge, ...(warning ? { warning } : {}) };
      const safeEvent = { from: { id: from.id }, data: safeData };
      if (!this.#authenticatedAcknowledgement(safeEvent, entry.authority)) continue;
      // Keep only the ordinary receipt and the authority fields that the SAME verifier needs.
      // Bridged message results are scrubbed at ingress; never retain an arbitrary result/body.
      const { result: _result, ...acceptance } = this.#acknowledgementAcceptance(safeData, entry.authority);
      if (acceptance.warning) Object.freeze(acceptance.warning);
      const acknowledgement: OwnerAcknowledgement = Object.freeze({ from: Object.freeze({ id: from.id }),
        data: Object.freeze({ ...acceptance, version: 1, commandId: entry.commandId, targetId: entry.authority.targetId,
          ...(data.ownerIncarnation !== undefined ? { ownerIncarnation: data.ownerIncarnation } : {}),
          ...(isIncarnation(data.staleIncarnation) || data.staleIncarnation === null
            ? { staleIncarnation: data.staleIncarnation } : {}),
          bridge: Object.freeze({ from: bridge.from }),
        }) });
      this.#relayAcknowledgements.delete(key);
      this.#relayAcknowledgements.set(key, Object.freeze({ ...entry, acknowledgement }));
      return; // preserve the refusal's absolute deadline, not a receipt-relative TTL
    }
  }

  /** Live and recovered owner receipts share the full owner/epoch/origin verifier. */
  #authenticatedAcknowledgement(event: OwnerAcknowledgement, pending: ControlAcknowledgementAuthority): Record<string, unknown> | undefined {
    if (!isObject(event.data) || typeof event.data.commandId !== "string") return undefined;
    if (
      event.data.version !== 1 ||
      event.data.targetId !== pending.targetId ||
      event.from.id !== pending.ownerIdentityId ||
      (event.data.ownerIncarnation !== undefined && !isIncarnation(event.data.ownerIncarnation)) ||
      // A correlated typed refusal may explicitly attest a missing wire epoch (null);
      // ordinary errors, successes, and an absent staleIncarnation never gain that authority.
      (pending.ownerIncarnation !== undefined &&
        event.data.ownerIncarnation !== pending.ownerIncarnation &&
        !(event.data.accepted === false && event.data.errorCode === CONTROL_STALE_INCARNATION &&
          event.data.error === STALE_INCARNATION_ERROR && isIncarnation(event.data.ownerIncarnation) &&
          (event.data.staleIncarnation === pending.ownerIncarnation || event.data.staleIncarnation === null))) ||
      // A validated mirror's answer authority survives record withdrawal/replacement.
      // Known native requests require an unstamped ACK, regardless of later metadata.
      // Only unbound legacy requests use the mutable metadata selector.
      !(pending.mirroredOwner
        ? isObject(event.data.bridge) && event.data.bridge.from === pending.mirroredOwner.remoteHost
        : pending.destinationRemoteHost === null
          ? !Object.prototype.hasOwnProperty.call(event.data, "bridge")
          : this.#bridgeMatches(pending.ownerHostId, event.data))
    ) {
      return undefined;
    }
    return event.data;
  }

  #settleAcknowledgement(commandId: string, pending: PendingControlRequest, data: Record<string, unknown>): void {
    if (this.#pending.get(commandId) !== pending) return;
    this.#clearPending(commandId);
    pending.resolve(this.#acknowledgementAcceptance(data, pending));
  }

  #acknowledgementAcceptance(data: Record<string, unknown>, pending: ControlAcknowledgementAuthority): FabricControlAcceptance {
    return {
      accepted: data.accepted === true,
      ...(data.accepted !== true && terminalRejectionFields(data)?.targetId === pending.targetId
        ? terminalRejectionFields(data) : {}),
      ...(typeof data.messageId === "string" ? { messageId: data.messageId } : {}),
      ...queueDepthOf(data),
      ...coalescedOf(data),
      ...runningTaskWarningOf(data, pending.targetId),
      ...triggeredOf(data),
      ...haltedOf(data),
      ...(Object.prototype.hasOwnProperty.call(data, "result") ? { result: data.result } : {}),
      ...(typeof data.error === "string" ? { error: data.error } : {}),
      ...(data.accepted === false && data.errorCode === CONTROL_STALE_INCARNATION
        ? { errorCode: CONTROL_STALE_INCARNATION } : {}),
      ...(data.errorCode === "FABRIC_INTERRUPT_NOT_AUTHORIZED" || data.errorCode === "FABRIC_INTERRUPT_RATE_LIMITED"
        ? { errorCode: data.errorCode } : {}),
      ...(pending.ownerMachine !== undefined && data.accepted === false &&
        (data.errorCode === "FABRIC_CONTROL_ID_CONFLICT" || data.errorCode === "FABRIC_SENDER_UNVERIFIED")
        ? { errorCode: data.errorCode } : {}),
      ...(data.accepted !== true && data.notRun === true ? { notRun: true as const } : {}),
    };
  }

  // NATS is deliberately a separate carrier branch. None of the methods below calls
  // the file reader, bridgeMatches, captureOwnerLease, shared claims, relayHub, timeout
  // withdrawal, file-log cleanup or file ACK readback (I-LOCK).

  #captureNatsOwner(ownerHostId: string, ownerIdentityId: string, targetId: string, epoch?: string): Readonly<ParticipantRoute> {
    const route = this.options.participants!.resolve(targetId);
    if (!route?.live || !/^[a-z0-9][a-z0-9-]{0,31}$/.test(route.machine)) {
      throw Object.assign(new TransportUnavailableError(`Unknown or unavailable NATS Fabric participant: ${targetId}`), { notRun: true });
    }
    if (route.ownerHostId !== ownerHostId || route.ownerIdentityId !== ownerIdentityId ||
        !isIncarnation(route.ownerIncarnation) || epoch !== undefined && route.ownerIncarnation !== epoch) {
      throw Object.assign(new FabricControlStaleIncarnationError(targetId), { notRun: true });
    }
    return Object.freeze({ ...route, capabilities: Object.freeze([...route.capabilities]) });
  }

  #sendNats<T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): Promise<TransportSent<T>> {
    // Do not await here: the adapter's synchronous body stamp and publish are P1/P2/P4/P5's tick.
    const sending = this.#transport.send(envelope, signal);
    this.#natsPublishes.add(sending);
    void sending.finally(() => this.#natsPublishes.delete(sending)).catch(() => undefined);
    return sending;
  }

  async #requestNatsAcceptance(
    ownerHostId: string, targetId: string, operation: FabricControlOperation, input: FabricControlInput,
    ownerIdentityId: string, options: FabricControlRequestOptions, retryBudgetSpentMs: number,
    notRunAttempt: number, requestCreatedAt: number, captured?: Readonly<ParticipantRoute>,
  ): Promise<{ commandId: string; acceptance: FabricControlAcceptance }> {
    assertSteerPriority(input.priority, operation);
    if (this.#closed) throw new Error("Fabric control plane closed");
    if (!this.options.enabled) throw new Error("Fabric mesh is disabled; cannot control a remote participant");
    if (this.#paused) throw Object.assign(new TransportUnavailableError("NATS control admission is draining"), { code: "FABRIC_TRANSPORT_DRAINING", notRun: true });
    options.signal?.throwIfAborted();
    const owner = captured ?? this.#captureNatsOwner(ownerHostId, ownerIdentityId, targetId, input.ownerIncarnation);
    const idempotencyKey = options.idempotencyKey ?? randomUUID();
    const commandBody = { targetId, operation, ownerIncarnation: owner.ownerIncarnation,
      ...(input.message !== undefined ? { message: input.message } : {}),
      ...(input.data !== undefined ? { data: input.data } : {}),
      ...(input.wakeCause !== undefined ? { wakeCause: input.wakeCause } : {}),
      ...(input.triggerTurn !== undefined ? { triggerTurn: input.triggerTurn } : {}),
      ...(input.priority !== undefined ? { priority: input.priority } : {}),
      ...(input.binding !== undefined ? { binding: input.binding } : {}),
      ...(input.bindingProvenance !== undefined ? { bindingProvenance: input.bindingProvenance } : {}),
    };
    const bodyFingerprint = controlBodyFingerprint(commandBody, input.principal);
    const originFingerprint = controlRequestFingerprint(operation, input);
    const commandId = createHash("sha256")
      .update(JSON.stringify([this.options.hostId, ownerHostId, ownerIdentityId, targetId, operation, idempotencyKey, notRunAttempt])).digest("hex");
    // Bind this caller key to its semantic request before any publication can occur.
    this.#bindOriginIdempotencyKey(commandId, idempotencyKey, originFingerprint);
    const existing = this.#pending.get(commandId);
    if (existing) {
      if (existing.bodyFingerprint !== bodyFingerprint) throw Object.assign(new Error("Fabric control id reused with different input"), { code: "FABRIC_CONTROL_ID_CONFLICT", notRun: true });
      throw new Error(`Fabric message with idempotencyKey ${idempotencyKey} is already awaiting acknowledgement`);
    }
    if (options.timeoutMs !== undefined && !Number.isFinite(options.timeoutMs)) throw new Error("Invalid NATS control timeout");
    const originalTimeoutMs = Math.min(NATS_HORIZON_MS, Math.max(this.#bridgeTimeoutMs,
      Math.floor(options.timeoutMs ?? this.#ackTimeoutMs)));
    const timeoutMs = Math.min(NATS_HORIZON_MS, Math.max(this.#pollMs * 4, originalTimeoutMs - retryBudgetSpentMs));
    const ackGraceMs = Math.min(MAX_CONTROL_ACK_GRACE_MS, 2 * timeoutMs);
    // E14.1: caller retries get new broker events; logical dedupe remains control-seen alone.
    // Any same-request republish must reuse this id and the resolved bytes, never restamp them.
    const commandEventId = randomUUID();
    let pending!: PendingControlRequest;
    const acceptance = new Promise<FabricControlAcceptance>((resolve, reject) => {
      pending = { resolve, reject, ownerHostId: owner.ownerHostId, ownerIdentityId: owner.ownerIdentityId,
        ownerMachine: owner.machine, ownerIncarnation: owner.ownerIncarnation, operation, targetId,
        commandPublished: false, requestCreatedAt, idempotencyKey, bodyFingerprint, commandEventId };
      this.#pending.set(commandId, pending);
      pending.timer = setTimeout(() => this.#timeoutPending(commandId), timeoutMs + ackGraceMs);
      pending.timer.unref();
      if (options.signal) {
        const abort = (): void => {
          const cancelled = this.#clearPending(commandId);
          const ceiling = operation === "ask" && options.detachOnMainCeiling ? mainExecutionCeilingAbortReason(options.signal) : undefined;
          if (cancelled && !ceiling) void this.#publishCancellation(commandId, cancelled).catch(() => undefined);
          reject(ceiling ?? new Error(`Remote Fabric request cancelled: ${targetId}`));
        };
        pending.signal = options.signal; pending.onAbort = abort;
        options.signal.addEventListener("abort", abort, { once: true });
        if (options.signal.aborted) abort();
      }
    });
    void acceptance.catch(() => undefined);
    try {
      const publishing = this.#sendNats({ id: commandEventId, kind: "command", to: { machine: owner.machine, hostId: owner.ownerHostId },
        targetId, operation, from: this.identity, principal: input.principal,
        body: (committedAt: number): FabricControlCommand => {
          // P1 and P2: never revive a cleared request or an aborted observation before publication.
          if (this.#pending.get(commandId) !== pending) throw new TransportUnavailableError("NATS control request is no longer pending");
          options.signal?.throwIfAborted();
          if (this.#closed || this.#paused) throw new TransportUnavailableError("NATS control admission is closed or draining");
          // P3's immutable, live ParticipantView snapshot is never re-resolved or retargeted.
          // The receiver's live lease + enforced incarnation close the preflight/PubAck race.
          return { version: 1, commandId, ...commandBody, replyTo: this.options.hostId,
            requestCreatedAt, requestedAt: committedAt, deadlineAt: committedAt + timeoutMs };
        },
      }, operation === "ask" && options.detachOnMainCeiling ? withoutMainExecutionCeiling(options.signal) : options.signal)
        .then(() => {
          pending.commandPublished = true;
          if (pending.cancellationRequested) void this.#publishCancellation(commandId, pending).catch(() => undefined);
        }, error => {
          if (error instanceof TransportUncertainError || isObject(error) && error.code === "FABRIC_TRANSPORT_UNCERTAIN") {
            pending.commandPublished = true;
            // Abort/timeout may already have cleared #pending while PubAck was in flight.
            if (pending.cancellationRequested) void this.#publishCancellation(commandId, pending).catch(() => undefined);
          }
          throw error;
        });
      void publishing.catch(() => undefined);
      await Promise.race([publishing, acceptance.then(() => undefined)]);
      const acknowledged = await acceptance;
      if (!acknowledged.accepted) {
        const terminal = terminalRejectionFields(acknowledged);
        if (terminal?.targetId === targetId) throw new FabricTargetTerminalError(targetId, terminal.finalAnswerReceiptId, terminal.runState);
        if (acknowledged.errorCode === CONTROL_STALE_INCARNATION) throw Object.assign(new FabricControlStaleIncarnationError(targetId), { notRun: acknowledged.notRun === true });
        const error = acknowledged.error || `Remote Fabric owner rejected command for ${targetId}`;
        throw new FabricControlRejection(acknowledged.notRun && error === CONTROL_COMMAND_EXPIRED
          ? `${error}; not delivered, safe to resend.` : error, acknowledged.notRun === true, acknowledged.errorCode);
      }
      return { commandId, acceptance: acknowledged };
    } catch (error) {
      const cancelled = this.#clearPending(commandId);
      if (cancelled) void this.#publishCancellation(commandId, cancelled).catch(() => undefined);
      throw error;
    }
  }

  async #publishNatsCancellation(commandId: string, pending: PendingControlRequest): Promise<void> {
    if (!pending.commandPublished) { pending.cancellationRequested = true; return; }
    if (pending.cancellationPublished) return;
    pending.cancellationPublished = true;
    const now = Date.now(), cancelCommandId = randomUUID(), cancelEventId = randomUUID();
    const body: FabricControlCommand = { version: 1, commandId: cancelCommandId, targetId: pending.targetId,
      operation: "cancel", cancelCommandId: commandId, replyTo: this.options.hostId,
      ownerIncarnation: pending.ownerIncarnation, requestCreatedAt: pending.requestCreatedAt,
      requestedAt: now, deadlineAt: now + Math.min(NATS_HORIZON_MS, this.#bridgeTimeoutMs) };
    await this.#backgroundCancellations.enqueue(async () => Date.now() > body.deadlineAt! ? undefined : this.#sendNats({
      id: cancelEventId, kind: "command", to: { machine: pending.ownerMachine!, hostId: pending.ownerHostId },
      targetId: pending.targetId, operation: "cancel", from: this.identity, body,
    }));
  }

  #natsSessionLive(session: AbortController): boolean {
    if (session.signal.aborted || this.#natsSession !== session || this.#closed || this.#paused || !this.options.enabled) return false;
    try {
      if (!this.options.participants!.leaseLive()) {
        if (this.options.participants!.leaseRecovering?.()) {
          // A timed-out lease is admission-held until the directory re-acquires it by CAS.
          this.#stopNatsInbox(session);
          this.#bindNatsInbox(true);
          return false;
        }
        // A conflicting owner or aborted lease is terminal, never a retry or auto-resume.
        this.pause(new MeshConsumptionPausedError());
        return false;
      }
      if (this.options.canConsumeMesh?.() !== false) return true;
    } catch {
      // Unknown gate/lease errors cannot authorize consumption or retry.
      this.pause(new MeshConsumptionPausedError());
      return false;
    }
    // Watch reset/readiness is an admission hold, not loss of the live lease.
    // Abort this iterator without clearing any explicit pause or bypassing gates.
    this.#stopNatsInbox(session);
    this.#bindNatsInbox(true);
    return false;
  }

  #assertNatsSession(session: AbortController): void {
    if (!this.#natsSessionLive(session)) throw new MeshConsumptionPausedError();
    this.#assertAdmission();
  }

  #trackNatsWork(work: Promise<void>): void {
    this.#natsWork.add(work);
    void work.finally(() => this.#natsWork.delete(work)).catch(() => undefined);
  }

  #startNats(): void {
    // E11: A2 owns the lease-loss notification and E8 reacquisition. No lease
    // polling, whenChanged(self), or automatic reacquisition lives in this plane.
    if (!this.#stopVerifiedReconnect && this.#transport.onVerifiedReconnect) {
      this.#stopVerifiedReconnect = this.#transport.onVerifiedReconnect(() => {
        this.#natsInboxRetryDelayMs = 1_000;
        if (this.#natsInboxRetryTimer) {
          clearTimeout(this.#natsInboxRetryTimer);
          this.#natsInboxRetryTimer = undefined;
        }
        this.#bindNatsInbox();
      });
    }
    if (!this.#natsWorkingTimer) {
      this.#natsWorkingTimer = setInterval(() => {
        const now = Date.now();
        for (const receipt of this.#natsReceipts) {
          if (now > receipt.workingUntil || !this.#natsSessionLive(receipt.session)) continue;
          try { receipt.delivery.working(); } catch { /* Keep custody; a duplicate joins its flight. */ }
        }
      }, NATS_WORKING_MS);
      this.#natsWorkingTimer.unref();
    }
    this.#bindNatsInbox();
    this.#scheduleNatsSeenCleanup();
  }

  #bindNatsInbox(retry = false): void {
    if (this.#closed || this.#paused || !this.#started || !this.options.enabled) return;
    if (retry) {
      if (this.#natsInboxRetryTimer) return;
      // B exposes no public reopen event. A failed entry/iteration or live
      // readiness hold alone arms one bounded one-shot; healthy idle never polls.
      this.#natsInboxRetryTimer = setTimeout(() => {
        this.#natsInboxRetryTimer = undefined;
        this.#bindNatsInbox();
      }, this.#natsInboxRetryDelayMs);
      this.#natsInboxRetryTimer.unref();
      this.#natsInboxRetryDelayMs = Math.min(30_000, this.#natsInboxRetryDelayMs * 2);
      return;
    }
    if (this.#natsInbox || this.#natsInboxRetryTimer) return;
    if (this.#natsOutcomes.size) { this.#scheduleNatsOutcomeRetry(); return; }
    try {
      if (!this.options.participants!.leaseLive()) {
        if (this.options.participants!.leaseRecovering?.()) { this.#bindNatsInbox(true); return; }
        this.pause(new MeshConsumptionPausedError()); return;
      }
      if (this.options.canConsumeMesh?.() === false) { this.#bindNatsInbox(true); return; }
    } catch (error) { this.pause(error); return; }
    const session = new AbortController();
    this.#natsSession = session;
    const consuming = (async () => {
      try {
        // A resumed iterator shares the bound with any still-settling old-session work.
        await this.#waitNatsCapacity(session);
        if (!this.#natsSessionLive(session)) return;
        const inbox = this.#transport.inbox(session.signal)[Symbol.asyncIterator]();
        try {
          for (;;) {
            const next = await inbox.next();
            if (next.done) return; // normal termination is not retryable
            if (!this.#natsSessionLive(session)) return; // leave this delivery unacked
            this.#natsInboxRetryDelayMs = 1_000;
            const receipt: NatsControlReceipt = { delivery: next.value, session, workingUntil: Infinity };
            this.#natsReceipts.add(receipt);
            this.#trackNatsWork(this.#receiveNats(receipt));
            await this.#waitNatsCapacity(session);
          }
        } finally { await inbox.return?.(); } // join before opening another iterator
      } catch (error) {
        if (!session.signal.aborted) {
          try { console.warn(`[pi-fabric] NATS control inbox stopped: ${error instanceof Error ? error.message : String(error)}`); } catch {}
          if (isObject(error) && error.code === "FABRIC_TRANSPORT_UNAVAILABLE") {
            if (this.#natsSessionLive(session)) {
              this.#stopNatsInbox(session);
              this.#bindNatsInbox(true);
            }
          } else this.pause(error);
        }
      } finally {
        if (!session.signal.aborted) this.pause(new TransportUnavailableError("NATS control inbox ended"));
        this.#stopNatsInbox(session);
      }
    })();
    this.#natsInbox = consuming;
    void consuming.finally(() => {
      if (this.#natsInbox === consuming) this.#natsInbox = undefined;
      if (this.#natsSession === session) this.#natsSession = undefined;
      // A permitted resume may arrive before the aborted old iterator has joined.
      // Lease-loss pause itself never reaches this branch without an explicit resume.
      if (!this.#paused && !this.#closed && this.#started) this.#bindNatsInbox();
    }).catch(() => undefined);
  }

  #stopNatsInbox(session = this.#natsSession, reason?: unknown): void {
    if (!session) return;
    session.abort(reason);
    for (const [key, flight] of this.#natsFlights) {
      if (flight.context.session !== session || flight.started) continue;
      clearTimeout(flight.expiryTimer);
      flight.admit(); flight.reject(new MeshConsumptionPausedError());
      this.#natsFlights.delete(key);
    }
    for (const lane of this.#natsLanes.values()) lane.queue = lane.queue.filter(flight => flight.context.session !== session);
    for (const wake of this.#natsCapacityWaiters) wake();
    this.#natsCapacityWaiters.clear();
  }

  async #waitNatsCapacity(session: AbortController): Promise<void> {
    while (this.#natsSessionLive(session) && (this.#natsReceipts.size >= NATS_IN_HAND_CAPACITY ||
        [...this.#natsLanes.values()].some(lane => lane.inHand >= NATS_LANE_CAPACITY))) {
      await new Promise<void>(resolve => { this.#natsCapacityWaiters.add(resolve); });
    }
  }

  #natsSenderVerified(context: NatsControlContext): boolean {
    const sender = this.options.participants!.resolve(context.event.from.id);
    return sender?.live === true && sender.machine === context.sourceMachine;
  }

  async #receiveNats(receipt: NatsControlReceipt): Promise<void> {
    const { delivery, session } = receipt;
    const terminate = async (reason: string): Promise<void> => {
      this.#assertNatsSession(session); // TERM is also a final broker disposition.
      await delivery.term(reason);
    };
    try {
      this.#assertNatsSession(session);
      const envelope = ownDataRecord(delivery.payload);
      const from = envelope && ownDataRecord(envelope.from);
      const to = envelope && ownDataRecord(envelope.to);
      const body = envelope && ownDataRecord(envelope.body);
      // Operator credentials take the relay-only path, regardless of the untrusted from label.
      // Relay-shaped traffic from a runtime credential is dropped by the same strict verifier.
      if (delivery.sourceMachine === this.#relayOperatorMachine ||
          typeof envelope?.id === "string" && envelope.id.startsWith("relay:") ||
          body?.errorCode === RELAY_DESTINATION_UNAVAILABLE || body?.errorCode === FABRIC_IDEMPOTENCY_KEY_REUSED) {
        this.#acceptNatsRelayRefusal(delivery);
        this.#assertNatsSession(session);
        await delivery.ack(); return;
      }
      if (!envelope || !from || !to || !body || !isNatsControlEventId(envelope.id) ||
          typeof envelope.targetId !== "string" || typeof envelope.operation !== "string" ||
          typeof from.id !== "string" || !from.id || to.machine !== this.#transport.self.machine ||
          to.hostId !== this.options.hostId || !/^[a-z0-9][a-z0-9-]{0,31}$/.test(delivery.sourceMachine)) {
        await terminate("Malformed NATS control envelope"); return;
      }
      if (envelope.kind === "ack") {
        // P6/ACK fast path: absolutely no directory/bridge/shared-state read here.
        this.#acceptNatsAcknowledgement(delivery);
        this.#assertNatsSession(session);
        await delivery.ack(); return;
      }
      if (envelope.kind !== "command" || !isNatsControlEventId(body.commandId) || envelope.targetId !== body.targetId ||
          envelope.operation !== body.operation || typeof body.requestedAt !== "number" || !Number.isFinite(body.requestedAt) ||
          typeof body.deadlineAt !== "number" || !Number.isFinite(body.deadlineAt) || body.deadlineAt < body.requestedAt ||
          typeof body.replyTo !== "string" || !body.replyTo) {
        await terminate("Malformed NATS control command"); return;
      }
      // Snapshot the raw admitted wire body before commandFromEvent hydrates wake metadata.
      const wire = JSON.parse(JSON.stringify(delivery.payload)) as TransportEnvelope;
      const bodyDigest = controlBodyFingerprint(wire.body, wire.principal);
      const event = { id: wire.id, sequence: delivery.sequence, createdAt: (body.requestedAt as number),
        topic: CONTROL_TOPIC, kind: wire.operation, to: this.options.hostId, from: wire.from,
        principal: wire.principal, ...(wire.sender ? { sender: wire.sender } : {}),
        data: wire.body, verification: delivery.verification } as MeshEvent;
      const command = commandFromEvent(event);
      if (!command || !command.commandId || !command.targetId) { await terminate("Malformed NATS control body"); return; }
      const deadlineAt = Math.min(command.deadlineAt!, command.requestedAt + NATS_HORIZON_MS);
      receipt.workingUntil = deadlineAt + MAX_CONTROL_ACK_GRACE_MS;
      const context: NatsControlContext = { command, event, sourceMachine: delivery.sourceMachine, deadlineAt, bodyDigest, session };
      if (!this.#natsSenderVerified(context)) {
        await this.#sendNatsAcknowledgement(context, { accepted: false, error: "Fabric control sender is unverified",
          errorCode: "FABRIC_SENDER_UNVERIFIED", notRun: true });
      } else {
        const target = this.options.participants!.resolve(command.targetId);
        // Capabilities, not an actor-id spelling heuristic, determine the shared actor lane.
        const laneKey = target?.capabilities.some(capability => capability === "ask" || capability === "tell")
          ? "actors" : `target:${command.targetId}`;
        let lane = this.#natsLanes.get(laneKey);
        if (!lane) { lane = { queue: [], inHand: 0, running: false }; this.#natsLanes.set(laneKey, lane); }
        receipt.lane = lane; lane.inHand++;
        const key = controlSeenKey(this.options.hostId, command.commandId);
        const existing = this.#natsFlights.get(key);
        if (existing) {
          if (existing.context.bodyDigest !== context.bodyDigest) {
            await this.#sendNatsConflict(context);
          } else if (existing.context.sourceMachine !== context.sourceMachine || existing.context.event.from.id !== event.from.id ||
              existing.context.command.replyTo !== command.replyTo) {
            await this.#sendNatsAcknowledgement(context, { accepted: false, error: "Fabric control sender is unverified",
              errorCode: "FABRIC_SENDER_UNVERIFIED", notRun: true });
          } else {
            // In particular, never confuse detached ASK admission with its durable settlement.
            await existing.completion;
          }
        } else {
          let admit!: () => void, resolve!: () => void, reject!: (error: unknown) => void;
          const admitted = new Promise<void>(done => { admit = done; });
          const completion = new Promise<void>((done, fail) => { resolve = done; reject = fail; });
          void completion.catch(() => undefined);
          const flight: NatsControlFlight = { context, started: false, admitted, admit, completion, resolve, reject };
          this.#natsFlights.set(key, flight);
          lane.queue.push(flight);
          // Expiry runs outside lane admission, including when a different command hangs.
          flight.expiryTimer = setTimeout(() => this.#launchNatsFlight(key, flight),
            Math.min(2_147_483_647, Math.max(1, deadlineAt - Date.now() + 1)));
          flight.expiryTimer.unref();
          this.#pumpNatsLanes(session);
          await completion;
        }
      }
      // B2: completion means local durable outcome AND ACK PubAck, never just handler admission.
      this.#assertNatsSession(session);
      await delivery.ack();
    } catch {
      // Pause/lease loss deliberately leaves every in-hand delivery untouched. A live
      // transient local-store/publish/ack failure gets bounded broker redelivery instead.
      if (this.#natsSessionLive(session)) { try { delivery.nak(1_000); } catch {} }
    } finally {
      this.#natsReceipts.delete(receipt);
      if (receipt.lane) receipt.lane.inHand--;
      for (const wake of this.#natsCapacityWaiters) wake();
      this.#natsCapacityWaiters.clear();
      this.#pruneNatsLanes();
    }
  }

  #pruneNatsLanes(): void {
    for (const [key, lane] of this.#natsLanes) {
      if (!lane.running && !lane.inHand && lane.queue.every(flight => flight.started || flight.context.session.signal.aborted)) this.#natsLanes.delete(key);
    }
  }

  #pumpNatsLanes(session: AbortController): void {
    if (!this.#natsSessionLive(session)) return;
    for (const lane of this.#natsLanes.values()) {
      if (this.#natsActiveLanes >= this.#inboxLanes) break;
      if (lane.running || !lane.queue.some(flight => !flight.started && !flight.context.session.signal.aborted)) continue;
      lane.running = true; this.#natsActiveLanes++;
      const running = (async () => {
        try {
          while (lane.queue.length && this.#natsSessionLive(session)) {
            const flight = lane.queue.shift()!;
            if (flight.started || flight.context.session !== session) continue;
            this.#launchNatsFlight(controlSeenKey(this.options.hostId, flight.context.command.commandId), flight);
            await flight.admitted;
          }
        } finally {
          lane.running = false; this.#natsActiveLanes--;
          this.#pruneNatsLanes();
          this.#pumpNatsLanes(session);
        }
      })();
      this.#trackNatsWork(running);
    }
  }

  #launchNatsFlight(key: string, flight: NatsControlFlight): void {
    if (flight.started || flight.context.session.signal.aborted) return;
    flight.started = true;
    clearTimeout(flight.expiryTimer);
    // An out-of-lane expiry must not leave its payload in a queue behind a hung
    // handler. Otherwise repeated expiries would evade the in-hand memory bound.
    for (const lane of this.#natsLanes.values()) lane.queue = lane.queue.filter(candidate => candidate !== flight);
    const settling = this.#settleNatsCommand(key, flight).then(flight.resolve, flight.reject).finally(() => {
      flight.admit();
      if (this.#natsFlights.get(key) === flight) this.#natsFlights.delete(key);
      this.#scheduleNatsSeenCleanup(flight.context.deadlineAt + NATS_HORIZON_MS);
    });
    this.#trackNatsWork(settling);
  }

  #acceptNatsRelayRefusal(delivery: TransportDelivery<TransportEnvelope>): void {
    const envelope = ownDataRecord(delivery.payload);
    const to = envelope && ownDataRecord(envelope.to);
    const data = envelope && ownDataRecord(envelope.body);
    const pending = data && typeof data.commandId === "string" ? this.#pending.get(data.commandId) : undefined;
    const fields = ["version", "commandId", "commandEventId", "targetId", "accepted", "error", "errorCode", "retryable"];
    if (delivery.sourceMachine !== this.#relayOperatorMachine || !envelope || !to || !data || !pending ||
        pending.ownerMachine === undefined || envelope.kind !== "ack" || envelope.operation !== "rejected" ||
        to.machine !== this.#transport.self.machine || to.hostId !== this.options.hostId ||
        envelope.id !== `relay:${pending.commandEventId}` || data.commandEventId !== pending.commandEventId ||
        envelope.targetId !== pending.targetId || data.targetId !== pending.targetId ||
        Object.keys(data).length !== fields.length || fields.some(field => !Object.hasOwn(data, field)) ||
        data.version !== 1 || data.accepted !== false || data.errorCode !== RELAY_DESTINATION_UNAVAILABLE ||
        data.error !== "Relay destination unavailable" || data.retryable !== true) {
      try { console.warn("[pi-fabric] Dropped invalid or unauthorized NATS operator relay refusal"); } catch {}
      return;
    }
    this.#clearPending(data.commandId as string);
    // NATS retains no UDS relay authority or late ACK cache. This is outcome unknown, never notRun.
    pending.reject(new FabricRelayDestinationUnavailableError(pending.targetId, pending.idempotencyKey));
  }

  #acceptNatsAcknowledgement(delivery: TransportDelivery<TransportEnvelope>): void {
    const envelope = delivery.payload, data = ownDataRecord(envelope.body);
    if (!data || typeof data.commandId !== "string" || !envelope.id.startsWith("ack:") ||
        !isNatsControlEventId(envelope.id.slice(4)) || typeof data.bodyDigest !== "string" || !/^[0-9a-f]{64}$/.test(data.bodyDigest) ||
        data.errorCode === RELAY_DESTINATION_UNAVAILABLE || data.errorCode === FABRIC_IDEMPOTENCY_KEY_REUSED ||
        envelope.targetId !== data.targetId || data.version !== 1 || typeof data.accepted !== "boolean" ||
        envelope.operation !== (data.accepted ? "accepted" : "rejected")) return;
    const pending = this.#pending.get(data.commandId);
    // P6 mirrors e931dbc8's ownerIdentityId and the paired from:this.identity ACK stamp.
    // Do NOT bind the ACK to pending.commandEventId: an earlier digest-equal attempt may settle this retry.
    if (!pending || pending.ownerMachine !== delivery.sourceMachine || envelope.from.id !== pending.ownerIdentityId ||
        data.targetId !== pending.targetId || data.bodyDigest !== pending.bodyFingerprint) return;
    if (data.ownerIncarnation !== pending.ownerIncarnation && !(data.accepted === false && data.notRun === true &&
        data.errorCode === CONTROL_STALE_INCARNATION && data.error === STALE_INCARNATION_ERROR && isIncarnation(data.ownerIncarnation) &&
        (data.staleIncarnation === pending.ownerIncarnation || data.staleIncarnation === null))) return;
    this.#settleAcknowledgement(data.commandId, pending, data);
  }

  #sendNatsConflict(context: NatsControlContext): Promise<void> {
    return this.#sendNatsAcknowledgement(context, { accepted: false, error: "Fabric control id reused with different input",
      errorCode: "FABRIC_CONTROL_ID_CONFLICT", notRun: true });
  }

  async #sendNatsAcknowledgement(context: NatsControlContext, acceptance: FabricControlAcceptance,
    ownerIncarnation: string | null = this.incarnation, staleIncarnation?: string | null): Promise<void> {
    const { command, session } = context;
    await this.#sendNats({ id: `ack:${context.event.id}`, kind: "ack",
      to: { machine: context.sourceMachine, hostId: command.replyTo }, targetId: command.targetId,
      // P6's ownerIdentityId is this identity, not the potentially different runtime inbox hostId.
      operation: acceptance.accepted ? "accepted" : "rejected", from: this.identity,
      body: () => {
        // P5: the lease/pause predicate is inside the same-tick stamp, not before an await.
        this.#assertNatsSession(session);
        return { ...acceptance, version: 1, commandId: command.commandId, targetId: command.targetId,
          bodyDigest: context.bodyDigest,
          ...(ownerIncarnation !== null ? { ownerIncarnation } : {}),
          ...(staleIncarnation !== undefined ? { staleIncarnation } : {}) };
      },
    });
  }

  async #persistNatsOutcome(key: string, retained: NatsRetainedOutcome): Promise<void> {
    try {
      await this.#seen.writeBatch({ identity: this.identity, ops: [{ kind: "put", key, ifVersion: retained.version,
        value: (writtenAt: number) => {
          // Evaluated under the local lock AFTER CAS: a conflicting retry keeps the last attempted stamp.
          retained.record = { ...retained.record, carrier: "nats", writtenAt };
          return retained.record;
        } }] });
    } catch (error) {
      // A write can have committed before its observer failed. Only a positive local
      // readback of this exact result reconciles it; never rerun a completed handler.
      let confirmed: FabricControlSeenRecord | undefined;
      try { confirmed = controlSeenRecord(this.#seen.get(key, { fresh: true })?.value); } catch {}
      if (!confirmed?.acceptance || confirmed.carrier !== "nats" || typeof retained.record.writtenAt !== "number" ||
          !Number.isFinite(retained.record.writtenAt) || confirmed.writtenAt !== retained.record.writtenAt ||
          confirmed.hostId !== retained.record.hostId || confirmed.commandId !== retained.record.commandId ||
          confirmed.targetId !== retained.record.targetId || confirmed.ownerIncarnation !== retained.record.ownerIncarnation ||
          confirmed.bodyDigest !== retained.record.bodyDigest || confirmed.sourceMachine !== retained.record.sourceMachine ||
          confirmed.requesterId !== retained.record.requesterId || confirmed.expiresAt < retained.record.expiresAt ||
          JSON.stringify(confirmed.acceptance) !== JSON.stringify(retained.record.acceptance)) {
        // Backpressure on a failed outcome store: do not release inputs and admit
        // an unbounded number of new unsaved results. Preserve this exact result.
        this.#stopNatsInbox();
        this.#scheduleNatsOutcomeRetry();
        throw error;
      }
    }
    if (this.#natsOutcomes.get(key) === retained) this.#natsOutcomes.delete(key);
  }

  #scheduleNatsOutcomeRetry(): void {
    if (this.#closed || this.#paused || this.#natsOutcomeRetryTimer || !this.#natsOutcomes.size) return;
    this.#natsOutcomeRetryTimer = setTimeout(() => {
      this.#natsOutcomeRetryTimer = undefined;
      const flushing = (async () => {
        for (const [key, outcome] of this.#natsOutcomes) await this.#persistNatsOutcome(key, outcome);
      })();
      this.#trackNatsWork(flushing);
      void flushing.catch(() => undefined).finally(() => {
        if (this.#natsOutcomes.size) this.#scheduleNatsOutcomeRetry();
        else this.#bindNatsInbox();
      });
    }, 1_000);
    this.#natsOutcomeRetryTimer.unref();
  }

  async #settleNatsCommand(key: string, flight: NatsControlFlight): Promise<void> {
    const context = flight.context, { command, session, deadlineAt } = context;
    this.#assertNatsSession(session);
    if (!this.#natsSenderVerified(context)) {
      await this.#sendNatsAcknowledgement(context, { accepted: false, error: "Fabric control sender is unverified",
        errorCode: "FABRIC_SENDER_UNVERIFIED", notRun: true }); return;
    }
    const retained = this.#natsOutcomes.get(key);
    if (retained) await this.#persistNatsOutcome(key, retained);
    let entry = this.#seen.get(key, { fresh: true });
    let record = controlSeenRecord(entry?.value);
    if (!record && entry) throw new Error("Invalid durable local control claim; refusing execution");
    if (!record) {
      if (command.ownerIncarnation !== this.incarnation) {
        await this.#sendNatsAcknowledgement(context, { accepted: false, error: STALE_INCARNATION_ERROR,
          errorCode: CONTROL_STALE_INCARNATION, notRun: true }, this.incarnation, command.ownerIncarnation ?? null);
        return;
      }
      record = { format: 1, carrier: "nats", hostId: this.options.hostId, commandId: command.commandId, targetId: command.targetId,
        ownerIncarnation: this.incarnation, expiresAt: deadlineAt + NATS_HORIZON_MS, explicitDeadline: true,
        bodyDigest: context.bodyDigest, sourceMachine: context.sourceMachine, requesterId: context.event.from.id };
      let claimVersion: number;
      try {
        // The ONLY claim lock is #seen's local lock. Check the lease under that lock,
        // immediately before the create, even without a supplied canConsumeMesh hook.
        const results = await this.#seen.writeBatch({ identity: this.identity, ops: [{ kind: "put", key,
          value: (writtenAt: number) => (record = { ...record!, carrier: "nats", writtenAt }), ifVersion: 0 }], beforeCommit: () => {
          this.#assertNatsSession(session);
          if (!this.#natsSenderVerified(context)) throw new MeshConsumptionPausedError();
        } });
        claimVersion = results[0]!.version;
      } catch (error) {
        if (error instanceof MeshConsumptionPausedError || isMeshWriteDeferred(error) || isLockTimeout(error)) throw error;
        entry = this.#seen.get(key, { fresh: true });
        record = controlSeenRecord(entry?.value);
        if (!record) throw error;
        // A concurrent claim, including a claim whose write response was lost, is
        // never evidence that this invocation may run its handler.
        await this.#replayNatsOutcome(key, context, record, entry!.version); return;
      }
      const controller = new AbortController();
      const abort = () => controller.abort();
      session.signal.addEventListener("abort", abort, { once: true });
      const timer = setTimeout(abort, Math.max(1, deadlineAt - Date.now())); timer.unref();
      this.#activeCommands.set(command.commandId, { controller, requesterId: context.event.from.id, targetId: command.targetId });
      let acceptance: FabricControlAcceptance;
      try {
        // Native custody admission has its own immediate fence, AFTER the local claim await.
        this.#assertNatsSession(session);
        if (!this.#natsSenderVerified(context)) throw new MeshConsumptionPausedError();
        if (Date.now() >= deadlineAt || command.requestedAt - Date.now() > this.#bridgeTimeoutMs) {
          acceptance = { accepted: false, error: CONTROL_COMMAND_EXPIRED, notRun: true };
        } else if (command.operation === "cancel") {
          this.#acceptCancellation(command, context.event.from); acceptance = { accepted: true };
        } else if (this.#handler) {
          const handling = this.#handler(command, context.event.from, controller.signal, context.event.verification);
          if (detachedControlOperation(command.operation)) flight.admit();
          acceptance = await handling;
        } else acceptance = { accepted: false, error: "Fabric owner has no control handler", notRun: true };
      } catch (error) {
        if (error instanceof MeshConsumptionPausedError) throw error;
        acceptance = { accepted: false, error: error instanceof Error ? error.message : String(error) };
      } finally {
        clearTimeout(timer); session.signal.removeEventListener("abort", abort);
        if (this.#activeCommands.get(command.commandId)?.controller === controller) this.#activeCommands.delete(command.commandId);
      }
      const outcome: NatsRetainedOutcome = { record: { ...record,
        acceptance: JSON.parse(JSON.stringify(this.#boundedAcceptance(acceptance))) as FabricControlAcceptance }, version: claimVersion };
      this.#natsOutcomes.set(key, outcome); // actual result custody precedes every awaited save
      // Outcomes may be saved after pause/lease loss under their original local CAS;
      // they must survive close, but no ACK or new native custody crosses that fence.
      await this.#persistNatsOutcome(key, outcome);
      await this.#sendNatsAcknowledgement(context, outcome.record.acceptance!);
      return;
    }
    await this.#replayNatsOutcome(key, context, record, entry!.version);
  }

  async #replayNatsOutcome(key: string, context: NatsControlContext, record: FabricControlSeenRecord, version: number): Promise<void> {
    if (record.hostId !== this.options.hostId || record.commandId !== context.command.commandId || record.targetId !== context.command.targetId ||
        record.bodyDigest !== undefined && record.bodyDigest !== context.bodyDigest) {
      await this.#sendNatsConflict(context); return; // absolutely no mutation of the original outcome
    }
    if (record.sourceMachine !== undefined && (record.sourceMachine !== context.sourceMachine || record.requesterId !== context.event.from.id)) {
      await this.#sendNatsAcknowledgement(context, { accepted: false, error: "Fabric control sender is unverified",
        errorCode: "FABRIC_SENDER_UNVERIFIED", notRun: true }); return;
    }
    const acceptance = record.acceptance ?? { accepted: false, error: "Fabric control outcome is indeterminate after owner restart" };
    if (!record.acceptance || record.expiresAt < context.deadlineAt + NATS_HORIZON_MS) {
      const outcome: NatsRetainedOutcome = { version, record: { ...record, carrier: "nats", acceptance,
        expiresAt: Math.max(record.expiresAt, context.deadlineAt + NATS_HORIZON_MS) } };
      this.#natsOutcomes.set(key, outcome);
      await this.#persistNatsOutcome(key, outcome);
    }
    // The responder authenticates this attempt; the retained epoch is history, not ACK authority.
    const replay = { ...acceptance, originalOwnerIncarnation: record.ownerIncarnation ?? null };
    await this.#sendNatsAcknowledgement(context, replay);
  }

  #scheduleNatsSeenCleanup(expiresAt?: number): void {
    if (this.#closed || this.#paused) return;
    if (expiresAt === undefined) {
      if (this.#natsCleanupTimer) return;
      expiresAt = Infinity;
      try {
        for (const entry of this.#seen.listAll(CONTROL_SEEN_PREFIX, { fresh: true })) {
          const record = controlSeenRecord(entry.value);
          if ((record?.carrier === "nats" || record?.bodyDigest !== undefined) && !this.#natsFlights.has(entry.key) && !this.#natsOutcomes.has(entry.key)) {
            expiresAt = Math.min(expiresAt, record.expiresAt);
          }
        }
      } catch { return; }
    }
    if (!Number.isFinite(expiresAt) || this.#natsCleanupTimer && this.#natsCleanupAt <= expiresAt) return;
    clearTimeout(this.#natsCleanupTimer);
    this.#natsCleanupAt = expiresAt;
    this.#natsCleanupTimer = setTimeout(() => {
      this.#natsCleanupTimer = undefined;
      const cleanup = (async () => {
        const now = Date.now();
        const stale = this.#seen.listAll(CONTROL_SEEN_PREFIX, { fresh: true }).filter(entry => {
          const record = controlSeenRecord(entry.value);
          return (record?.carrier === "nats" || record?.bodyDigest !== undefined) && record.expiresAt < now && !this.#natsFlights.has(entry.key) && !this.#natsOutcomes.has(entry.key);
        });
        for (let start = 0; start < stale.length && !this.#closed && !this.#paused; start += 64) {
          await this.#seen.writeBatch({ identity: this.identity, ops: stale.slice(start, start + 64).map(entry => ({
            kind: "delete" as const, key: entry.key, ifVersion: entry.version, onConflict: "skip" as const })) });
        }
      })();
      this.#trackNatsWork(cleanup);
      void cleanup.catch(() => undefined).finally(() => {
        // Failure gets a bounded retry rather than a spent-expiry microtask loop.
        if (!this.#closed && !this.#paused) {
          clearTimeout(this.#natsCleanupTimer);
          this.#natsCleanupTimer = setTimeout(() => {
            this.#natsCleanupTimer = undefined;
            this.#scheduleNatsSeenCleanup();
          }, 1_000);
          this.#natsCleanupTimer.unref();
        }
      });
    }, Math.min(2_147_483_647, Math.max(1, expiresAt - Date.now() + 1)));
    this.#natsCleanupTimer.unref();
  }

  #readNatsReleaseOutcomes(): unknown {
    // E17: in-process joins/drain do not replace a receipt covering each marked record's last write.
    const localOutcomes = this.#seen.listAll(CONTROL_SEEN_PREFIX, { fresh: true })
      .filter(entry => this.#releaseNeedsFileEvidence(entry.value)).sort((a, b) => a.key.localeCompare(b.key));
    if (localOutcomes.some(entry => !controlSeenRecord(entry.value)?.acceptance)) throw new Error("NATS control has a local claim without a durable outcome");
    // E22: only retained file-era local records need the shared log; marked-only
    // NATS histories must not touch it or read shared claims at release.
    if (localOutcomes.length) this.#readFileReleaseAcknowledgements(localOutcomes);
    return localOutcomes;
  }

  async #checkpointNatsForRelease(valid?: () => boolean): Promise<void> {
    const epoch = this.#releaseEpoch;
    const check = () => {
      if (!this.#paused || epoch !== this.#releaseEpoch || valid?.() === false) throw new Error("Control release checkpoint is stale or not paused");
    };
    check();
    await this.#natsInbox; check();
    await Promise.allSettled([...this.#natsWork]); check();
    await this.#backgroundCancellations.checkpointForRelease(); check();
    await Promise.allSettled([...this.#natsPublishes]); check();
    for (const [key, outcome] of this.#natsOutcomes) { await this.#persistNatsOutcome(key, outcome); check(); }
    if (this.#pending.size || this.#resendWaits.size || this.#activeCommands.size || this.#natsReceipts.size || this.#natsFlights.size ||
        this.#natsWork.size || this.#natsPublishes.size || this.#natsOutcomes.size) throw new Error("NATS control release has unsettled obligations");
    const drain = await this.#transport.drainState(); check();
    if (drain.pending !== 0 || drain.ackPending !== 0) throw new Error("NATS control inbox has not drained");
    this.#natsReleaseDrain = { ...drain };
    const state = JSON.stringify(this.#readReleaseState()); check();
    this.#releaseCheckpoint = { epoch, state };
  }

  async #closeNats(): Promise<void> {
    this.#closed = true; this.#started = false;
    this.#stopVerifiedReconnect?.();
    this.#stopVerifiedReconnect = undefined;
    clearTimeout(this.#natsInboxRetryTimer); this.#natsInboxRetryTimer = undefined;
    for (const cancel of this.#resendWaits) cancel();
    clearInterval(this.#natsWorkingTimer); this.#natsWorkingTimer = undefined;
    clearTimeout(this.#natsCleanupTimer); this.#natsCleanupTimer = undefined;
    clearTimeout(this.#natsOutcomeRetryTimer); this.#natsOutcomeRetryTimer = undefined;
    this.#stopNatsInbox();
    await this.#natsInbox;
    const cancellations: Promise<void>[] = [];
    for (const id of [...this.#pending.keys()]) {
      const pending = this.#clearPending(id);
      if (!pending) continue;
      cancellations.push(this.#publishCancellation(id, pending));
      pending.resolve({ accepted: false, error: "Fabric control plane closed" });
    }
    await Promise.allSettled(cancellations);
    await Promise.allSettled([...this.#natsWork]);
    // Joining publishes may enqueue the cancellation of a command that committed after close.
    while (this.#natsPublishes.size) await Promise.allSettled([...this.#natsPublishes]);
    await this.#backgroundCancellations.checkpointForRelease();
    for (const [key, outcome] of this.#natsOutcomes) await this.#persistNatsOutcome(key, outcome);
    this.#handler = undefined;
    // The composition root owns transport.close(); no file drain/readback/cleanup here.
  }

  // smarty-dev#2004: an owner mirrored from remote host R answers only through R's mesh bridge,
  // which stamps data.bridge.from = R; a native owner's answer never carries a bridge stamp. So a
  // faulty bridge cannot answer for another link's owner, or for one of this mesh's own.
  #bridgeMatches(ownerHostId: string, data: Record<string, unknown>): boolean {
    const host = this.mesh.get(HOST_PREFIX + createHash("sha256").update(ownerHostId).digest("hex"))?.value;
    const remoteHost = isObject(host) && host.id === ownerHostId && typeof host.remoteHost === "string"
      ? host.remoteHost
      : undefined;
    if (!Object.prototype.hasOwnProperty.call(data, "bridge")) return remoteHost === undefined;
    return remoteHost !== undefined && isObject(data.bridge) && data.bridge.from === remoteHost;
  }

  async #acceptCommand(event: MeshEvent): Promise<void> {
    assertMeshConsumption(this.options.canConsumeMesh);
    const command = commandFromEvent(event);
    if (!command) return;
    const ownedKey = controlSeenKey(this.options.hostId, command.commandId);
    const owned = this.#ownedCommands.get(ownedKey);
    if (owned) {
      if ("claimVersion" in owned && !owned.acknowledgement && !owned.running) await this.#runOwnedCommand(ownedKey, owned);
      return;
    }
    this.#assertAdmission();
    // Explicit epochs always fence stale owners. Omission remains compatible only
    // during warn-mode rollout; timestamps cannot substitute for an epoch in enforce.
    const incarnationMatches = command.ownerIncarnation === this.incarnation ||
      (command.ownerIncarnation === undefined && this.options.controlIncarnationFence !== "enforce");
    if (command.operation === "cancel") {
      if (incarnationMatches) this.#acceptCancellation(command, event.from);
      return;
    }
    const now = Date.now();
    const deadlineAt = Math.min(
      command.deadlineAt ?? command.requestedAt + this.#ackTimeoutMs,
      command.requestedAt + MAX_CONTROL_TIMEOUT_MS,
    );
    const key = ownedKey;
    // Historical replay has no waiting sender; keep relaunches free of redundant ACK writes. A command a
    // bridge delivered late (its bridged copy was published here just now, smarty-dev#9751) is not history:
    // it gets the expired answer below, never a silent skip, so its sender and the fleet see it.
    const lateDelivery = isObject(event.data) && isObject(event.data.bridge) &&
      now - event.createdAt <= MAX_CONTROL_ACK_GRACE_MS + this.#pollMs * 4;
    if (now > deadlineAt + MAX_CONTROL_ACK_GRACE_MS + this.#pollMs * 4 && !lateDelivery) return;
    // Dedupe precedes the new-admission fence: completed earlier outcomes remain observable,
    // but their ACK names the claiming activation, never this publisher's new activation.
    const duplicate = this.#seenRecord(ownedKey);
    if (duplicate) {
      if (duplicate.hostId === this.options.hostId && duplicate.commandId === command.commandId &&
        duplicate.targetId === command.targetId) {
        if (duplicate.acceptance && (command.operation === "steer" || command.operation === "followUp")) {
          this.#ownedCommands.set(ownedKey, { event, acceptance: duplicate.acceptance,
            // A worker may already exist: queued replay custody is running too, not retry work.
            running: true, acknowledgement: { command, ownerIncarnation: duplicate.ownerIncarnation ?? null } });
          this.#resumeAcknowledgements();
        } else {
          await this.#publishAcknowledgement(command, duplicate.acceptance ?? {
            accepted: false, error: "Fabric control outcome is indeterminate after owner restart",
          }, duplicate.ownerIncarnation ?? null);
        }
      }
      return;
    }
    // Stale wire epochs cannot downgrade a new admission in either mode. Enforce
    // also refuses omission; completed predecessor claims above remain observable.
    if (!incarnationMatches) {
      await this.#publishAcknowledgement(command, {
        accepted: false, error: STALE_INCARNATION_ERROR, errorCode: CONTROL_STALE_INCARNATION,
      }, this.incarnation, command.ownerIncarnation ?? null);
      return; // no claim, handler, or automatic notRun resend
    }
    try {
      // One claim authority across drainers, older runtimes and sender withdrawal.
      // The private store is only an outcome store, never a second admission claim.
      await this.#putFenced(this.mesh, {
        key,
        value: {
          format: 1,
          hostId: this.options.hostId,
          commandId: command.commandId,
          targetId: command.targetId,
          ownerIncarnation: this.incarnation,
          expiresAt: Math.max(deadlineAt, now) + this.#ackTimeoutMs,
          ...(command.deadlineAt !== undefined ? { explicitDeadline: true } : {}),
        } satisfies FabricControlSeenRecord,
        identity: this.identity,
        ifVersion: 0,
      }, true);
    } catch (error) {
      if (isMeshWriteDeferred(error) || isLockTimeout(error) || error instanceof MeshConsumptionPausedError) throw error;
      // A pre-CAS read may be cached; only fresh outcome/claim readback can answer the loser.
      const raced = this.#seenRecord(key, true);
      if (
        raced?.hostId === this.options.hostId &&
        raced.commandId === command.commandId &&
        raced.targetId === command.targetId
      ) {
        await this.#publishAcknowledgement(
          command,
          raced.acceptance ?? {
            accepted: false,
            error: "Fabric control outcome is indeterminate after concurrent claim",
          },
          raced.ownerIncarnation ?? null,
        );
      }
      return;
    }

    // Take custody before any post-CAS admission check: PREPARE/lease loss during
    // the await must retain this command. Its first private outcome save creates it.
    const admitted = { event, claimVersion: 0, futureSkewed: command.requestedAt - now > this.#ackTimeoutMs };
    this.#ownedCommands.set(key, admitted);
    await this.#runOwnedCommand(key, admitted);
  }

  async #runOwnedCommand(key: string, owned: ClaimedControlCommand): Promise<void> {
    if (!owned.acceptance) this.#assertAdmission();
    else assertMeshConsumption(this.options.canConsumeMesh);
    const command = commandFromEvent(owned.event)!;
    const deadlineAt = Math.min(command.deadlineAt ?? command.requestedAt + this.#ackTimeoutMs, command.requestedAt + MAX_CONTROL_TIMEOUT_MS);
    owned.running = true;
    const execution = this.#executeClaimedCommand(command, owned.event.from, key, owned, deadlineAt, owned.event.sequence, owned.event.verification)
      .catch(error => {
        if (detachedControlOperation(command.operation) && isLockTimeout(error)) this.#backgroundPoll.failure(error);
        throw error;
      })
      .finally(() => { if (!owned.acknowledgement) owned.running = false; });
    if (detachedControlOperation(command.operation)) {
      this.#activeHandlers.add(execution);
      void execution.finally(() => {
        this.#activeHandlers.delete(execution);
        this.#retryRetainedOwned(key, owned);
      }).catch(() => undefined);
      return;
    }
    await execution;
  }

  #warnUnfencedSenderDelivery(command: FabricControlCommand, from: MeshIdentity): void {
    if (command.ownerIncarnation !== undefined) return;
    const now = Date.now();
    const previous = this.#unfencedSenders.get(from.id);
    const count = (previous?.count ?? 0) + 1;
    if (previous && now - previous.warnedAt < UNFENCED_SENDER_WARNING_MS) {
      previous.count = count;
      return;
    }
    // Retain only this hour's senders, without a timer or an unbounded historical map.
    for (const [id, record] of this.#unfencedSenders) {
      if (now - record.warnedAt >= UNFENCED_SENDER_WARNING_MS) this.#unfencedSenders.delete(id);
    }
    this.#unfencedSenders.set(from.id, { count, warnedAt: now });
    const message = `Unfenced legacy sender delivery count=${count} sender=${JSON.stringify(from.id)} operation=${command.operation} target=${JSON.stringify(command.targetId)} mesh.controlIncarnationFence=warn`;
    try {
      if (this.options.onUnfencedSenderWarning) this.options.onUnfencedSenderWarning(message);
      else diagnostics.warn("src/topology/control-plane.ts", `[pi-fabric] ${message}`);
    } catch { /* A diagnostic sink never fails an admitted delivery. */ }
  }

  #acceptCancellation(command: FabricControlCommand, from: MeshIdentity): void {
    if (!command.cancelCommandId) return;
    const active = this.#activeCommands.get(command.cancelCommandId);
    if (
      active &&
      active.requesterId === from.id &&
      active.targetId === command.targetId
    ) {
      this.#warnUnfencedSenderDelivery(command, from);
      active.controller.abort();
    }
  }

  #acknowledgementsFull(): boolean {
    let count = 0;
    for (const owned of this.#ownedCommands.values()) if (owned.acknowledgement && ++count >= 100) return true;
    return false;
  }

  /** One owned worker, not a best-effort queue or one lock waiter per command. */
  #resumeAcknowledgements(): void {
    if (this.#ackWorker || ![...this.#ownedCommands.values()].some(owned => owned.acknowledgement)) return;
    for (const owned of this.#ownedCommands.values()) if (owned.acknowledgement) owned.running = true;
    let failed = false;
    const execution = this.#publishSavedAcknowledgements().catch(error => { failed = true; throw error; });
    this.#ackWorker = execution;
    this.#activeHandlers.add(execution);
    void execution.finally(() => {
      this.#ackWorker = undefined;
      this.#activeHandlers.delete(execution);
      if (!failed) { this.#resumeAcknowledgements(); return; }
      let retained: [string, OwnedControlCommand] | undefined;
      for (const [key, owned] of this.#ownedCommands) {
        if (!owned.acknowledgement) continue;
        owned.running = false;
        retained ??= [key, owned];
      }
      if (retained) this.#retryRetainedOwned(...retained); // one existing backoff for the worker
    }).catch(() => undefined);
  }

  async #publishSavedAcknowledgements(): Promise<void> {
    for (;;) {
      const next = [...this.#ownedCommands].find(([, owned]) => owned.acknowledgement);
      if (!next) return;
      const [key, owned] = next;
      const { command, ownerIncarnation } = owned.acknowledgement!;
      try {
        await this.#publishAcknowledgement(command, owned.acceptance!, ownerIncarnation);
        this.#ownedCommands.delete(key);
        this.#wake(); // settlement resumes the unread command at the capacity fence
      } catch (error) {
        if (isLockTimeout(error)) this.#backgroundPoll.failure(error);
        const deferred = isMeshWriteDeferred(error) || isLockTimeout(error) || error instanceof MeshConsumptionPausedError;
        if (!deferred) {
          this.#ackPublicationFailed = this.#releasePublicationFailed = true;
          this.#uncertainAcknowledgement ??= { commandId: command.commandId, outcomeKey: key };
          // Keep the saved outcome; a later duplicate may observe it, never run its handler.
          this.#ownedCommands.delete(key);
          this.#wake();
        }
        throw error;
      }
    }
  }

  async #executeClaimedCommand(
    command: FabricControlCommand,
    from: MeshIdentity,
    key: string,
    owned: ClaimedControlCommand,
    deadlineAt: number,
    sequence: number,
    verification: MeshEvent["verification"],
  ): Promise<void> {
    const outcomeIncarnation = this.incarnation;
    const controller = new AbortController();
    this.#activeCommands.set(command.commandId, {
      controller,
      requesterId: from.id,
      targetId: command.targetId,
    });
    const deadlineTimer = setTimeout(
      () => controller.abort(),
      Math.max(1, deadlineAt - Date.now()),
    );
    deadlineTimer.unref();
    try {
      let acceptance: FabricControlAcceptance;
      try {
        // The claim can wait for a lock, or this process can pause after it commits: the deadline
        // may have passed since admission. An expired command is recorded, not run: notRun, as
        // this runtime holds the claim and the handler did not run.
        assertMeshConsumption(this.options.canConsumeMesh);
        if (!owned.acceptance) this.#assertAdmission();
        if (owned.acceptance) acceptance = owned.acceptance;
        else if (Date.now() > deadlineAt || owned.futureSkewed || command.requestedAt - Date.now() > this.#ackTimeoutMs) {
          acceptance = { accepted: false, error: CONTROL_COMMAND_EXPIRED, notRun: true };
        }
        else if (this.#handler) {
          this.#warnUnfencedSenderDelivery(command, from);
          acceptance = await this.#handler(command, from, controller.signal, verification);
        } else acceptance = { accepted: false, error: "Fabric owner has no control handler" };
      } catch (error) {
        if (error instanceof MeshConsumptionPausedError) throw error;
        acceptance = {
          accepted: false,
          error: error instanceof Error ? error.message : String(error),
        };
      }
      acceptance = this.#boundedAcceptance(acceptance);
      const acknowledgement = command.operation === "steer" || command.operation === "followUp"
        ? { command: { ...command }, ownerIncarnation: outcomeIncarnation } : undefined;
      // Snapshotting can invoke user result.toJSON again and throw. Retain the completed
      // result first, so a transient serialization failure retries the save, never the handler.
      owned.acceptance = acceptance;
      if (acknowledgement) {
        // Match the saved JSON value, not a handler-owned object that can mutate while queued.
        acceptance = JSON.parse(JSON.stringify(acceptance)) as FabricControlAcceptance;
      }
      owned.acceptance = acceptance; // custody precedes any awaited sequence/ACK commit
      assertMeshConsumption(this.options.canConsumeMesh);
      const saveOutcome = () => this.#putFenced(this.#seen, {
          key,
          value: {
            format: 1,
            hostId: this.options.hostId,
            commandId: command.commandId,
            targetId: command.targetId,
            ownerIncarnation: outcomeIncarnation,
            expiresAt: Math.max(deadlineAt, Date.now()) + this.#ackTimeoutMs,
            sequence,
            acceptance,
          } satisfies FabricControlSeenRecord,
          identity: this.identity,
          ifVersion: owned.claimVersion,
        });
      try {
        if (!owned.saved) { await saveOutcome(); owned.saved = true; }
      } catch (error) {
        if (error instanceof MeshConsumptionPausedError) throw error;
        const deferred = isMeshWriteDeferred(error) || isLockTimeout(error);
        if (!deferred) this.#releasePublicationFailed = true;
        // A conflict belongs to another owner; a timeout wrote nothing. Preserve the actual
        // result under the original version fence and retry it without running the handler.
        if (!deferred) { this.#ownedCommands.delete(key); return; }
        throw error; // the owned outcome is retried by the next confirmed drain
      }
      if (acknowledgement) {
        owned.acknowledgement = acknowledgement;
        this.#resumeAcknowledgements();
        return; // only the saved ACK is offloaded; the handler and save stayed reader-serial
      }
      try {
        await this.#publishAcknowledgement(command, acceptance);
        this.#ownedCommands.delete(key);
      } catch (error) {
        const deferred = isMeshWriteDeferred(error) || isLockTimeout(error) || error instanceof MeshConsumptionPausedError;
        if (!deferred) {
          this.#releasePublicationFailed = true;
          // An indeterminate permanent ACK must not hot-loop or re-run a completed handler.
          // A later duplicate command can retry the durable outcome under its own event.
          this.#ownedCommands.delete(key);
        }
        throw error; // preserve known no-write outcomes for bounded retry
      }
    } catch (error) {
      if (error instanceof MeshConsumptionPausedError && owned.acceptance && !owned.saved) {
        // Outcome custody is not a cursor commit. Keep the real completed result
        // in our host-local claim WITHOUT a sequence even while consumption is
        // fenced, so a restart can answer it rather than replaying the handler.
        // The same CAS still owns it; shutdown retirement can veto the write.
        try {
          const retained = await this.#seen.put({ key, identity: this.identity, ifVersion: owned.claimVersion,
            value: { format: 1, hostId: this.options.hostId, commandId: command.commandId,
              targetId: command.targetId, ownerIncarnation: outcomeIncarnation, expiresAt: Math.max(deadlineAt, Date.now()) + this.#ackTimeoutMs,
              acceptance: owned.acceptance } satisfies FabricControlSeenRecord });
          owned.claimVersion = retained.version;
        } catch { /* The owned in-memory outcome and durable claim remain; never replay. */ }
      }
      throw error;
    } finally {
      clearTimeout(deadlineTimer);
      const active = this.#activeCommands.get(command.commandId);
      if (active?.controller === controller) this.#activeCommands.delete(command.commandId);
    }
  }

  #boundedAcceptance(acceptance: FabricControlAcceptance): FabricControlAcceptance {
    try {
      const budget = this.mesh.maxEventBytes - 2_048;
      if (controlAcceptanceBytes(acceptance) <= budget) {
        return acceptance;
      }
      if (acceptance.accepted && acceptance.warning) {
        // Admission has already committed the delivery. An optional advisory must
        // not turn its successful receipt into a refusal (and invite a duplicate
        // retry). Keep all delivery fields, omitting only the warning if it fits.
        const { warning: _warning, ...delivery } = acceptance;
        if (controlAcceptanceBytes(delivery) <= budget) return delivery;
      }
    } catch {
      // Return a bounded rejection below.
    }
    return {
      accepted: false,
      error: `Fabric control result exceeds ${this.mesh.maxEventBytes} mesh event bytes`,
    };
  }

  // This host's record for a command: its own store (with the outcome) first, then the shared
  // state, where every claim is also made and where runtimes before smarty-dev#643 keep outcomes.
  #seenRecord(key: string, fresh = false): FabricControlSeenRecord | undefined {
    return controlSeenRecord(this.#seen.get(key, { fresh })?.value) ?? controlSeenRecord(this.mesh.get(key, { fresh })?.value);
  }

  async #cleanupSeen(now: number): Promise<void> {
    // Read inside the owned drain so a state outage uses its bounded retry, not
    // an unhandled scheduling error. No shared records means no cleanup timer.
    this.#legacySeenPending = this.mesh.listAll(CONTROL_SEEN_PREFIX).length > 0;
    if (now - this.#seenCleanupAt < this.#ackTimeoutMs) return;
    this.#seenCleanupAt = now;
    // A restarting owner replays the retained log from its start, so a record stays while its
    // command is still in the log: past its expiry, and below the log's oldest sequence.
    const oldest = this.mesh.oldestSequence();
    const stale = this.#seen.listAll(CONTROL_SEEN_PREFIX).filter((entry) => {
      if (this.#ownedCommands.has(entry.key)) return false;
      const record = controlSeenRecord(entry.value);
      // E14-X1: NATS records have no file-log replay obligation, even with an empty file log.
      if (record?.carrier === "nats") return record.expiresAt < now;
      return oldest !== undefined && (!record || (record.expiresAt < now && record.sequence !== undefined && record.sequence < oldest));
    });
    for (let start = 0; start < stale.length; start += 64) {
      await this.#seen.writeBatch({
        identity: this.identity,
        ops: stale.slice(start, start + 64).map((entry) => ({ kind: "delete" as const, key: entry.key, ifVersion: entry.version, onConflict: "skip" as const })),
      });
    }
    if (now - this.#legacySeenCleanupAt >= LEGACY_SEEN_CLEANUP_MS) {
      this.#legacySeenCleanupAt = now;
      await this.#cleanupLegacySeen(now);
      this.#legacySeenPending = this.mesh.listAll(CONTROL_SEEN_PREFIX).length > 0;
    }
  }

  // Shared-state records, any host's (smarty-dev#643, #816). By default one goes once it has
  // expired and its command has left the event log: a runtime before phase 1 (before Fabric
  // B8) checks the deadline only at admission and knows no other fence, so it could pass
  // admission, pause, and claim again after an earlier deletion (review/astra on #65).
  // ponytail: the log rule kept 2,858 expired claims (46% of the shared state) and saturated
  // the mesh lock, because the log compacts only at 64 MiB. Once no runtime before phase 1
  // remains on the root, the fleet owner sets CONTROL_CLAIMS_POLICY_KEY to
  // { version: 1, sharedClaims: "expiry" }; then a claim for a command with its own deadline goes
  // 10 minutes after it expires. That ends support for those runtimes on this root: one started
  // afterwards could run a command twice in that pause. Runtime versions cannot be told apart
  // automatically, since two processes of one host write the same lease key.
  async #cleanupLegacySeen(now: number): Promise<void> {
    const expired = this.mesh.listAll(CONTROL_SEEN_PREFIX).flatMap((entry) => {
      if (this.#ownedCommands.has(entry.key)) return [];
      const record = controlSeenRecord(entry.value);
      return !record || record.expiresAt < now ? [{ entry, record }] : [];
    });
    if (expired.length === 0) return;
    const policy = this.mesh.get(CONTROL_CLAIMS_POLICY_KEY, { fresh: true })?.value;
    const expiryReclaim = isObject(policy) && policy.version === 1 && policy.sharedClaims === "expiry";
    const reclaimable = ({ record }: { record: FabricControlSeenRecord | undefined }): boolean =>
      expiryReclaim && record?.explicitDeadline === true && record.expiresAt + SHARED_SEEN_GRACE_MS < now;
    const dead = expired.filter(reclaimable);
    const unflagged = expired.filter((candidate) => !reclaimable(candidate));

    if (unflagged.length > 0) {
      const sought = new Set(unflagged.flatMap(({ record }) => record ? [record.commandId] : []));
      const retained = new Set<string>();
      let offset = 0;
      while (sought.size > retained.size) {
        const page = this.mesh.tail(offset, this.mesh.maxReadEvents);
        for (const event of page.events) {
          if (event.topic !== CONTROL_TOPIC || !isObject(event.data)) continue;
          const commandId = event.data.commandId;
          if (typeof commandId === "string" && sought.has(commandId)) retained.add(commandId);
        }
        if (page.events.length < this.mesh.maxReadEvents || page.nextOffset === offset) break;
        offset = page.nextOffset;
      }
      dead.push(...unflagged.filter(({ record }) => !record || !retained.has(record.commandId)));
    }
    if (dead.length === 0) return;
    // Independent claim retirements: keep each hold bounded, preserving every version fence.
    for (let start = 0; start < dead.length; start += 64) {
      await this.mesh.writeBatch({
        identity: this.identity,
        ops: dead.slice(start, start + 64).map(({ entry }) => ({ kind: "delete" as const, key: entry.key, ifVersion: entry.version, onConflict: "skip" as const })),
      });
    }
  }

  async #publishAcknowledgement(
    command: FabricControlCommand,
    acceptance: FabricControlAcceptance,
    ownerIncarnation: string | null = this.incarnation,
    // null explicitly attests that this command arrived without an epoch.
    staleIncarnation?: string | null,
  ): Promise<void> {
    await this.#fileTransport.publish({
        topic: ACK_TOPIC,
        kind: acceptance.accepted ? "accepted" : "rejected",
        from: this.identity,
        to: command.replyTo,
        data: () => {
          assertMeshConsumption(this.options.canConsumeMesh);
          return {
          version: 1,
          commandId: command.commandId,
          targetId: command.targetId,
          ...(ownerIncarnation !== null ? { ownerIncarnation } : {}),
          ...(staleIncarnation !== undefined ? { staleIncarnation } : {}),
          accepted: acceptance.accepted,
          ...(acceptance.messageId ? { messageId: acceptance.messageId } : {}),
          ...queueDepthOf(acceptance as unknown as Record<string, unknown>),
          ...coalescedOf(acceptance as unknown as Record<string, unknown>),
          ...runningTaskWarningOf(acceptance as unknown as Record<string, unknown>, command.targetId),
          ...triggeredOf(acceptance as unknown as Record<string, unknown>),
          ...haltedOf(acceptance as unknown as Record<string, unknown>),
          ...(Object.prototype.hasOwnProperty.call(acceptance, "result")
            ? { result: acceptance.result }
            : {}),
          ...(acceptance.error ? { error: acceptance.error } : {}),
          ...(acceptance.errorCode ? { errorCode: acceptance.errorCode } : {}),
          ...(!acceptance.accepted && terminalRejectionFields(acceptance)?.targetId === command.targetId
            ? terminalRejectionFields(acceptance) : {}),
          ...(!acceptance.accepted && acceptance.notRun ? { notRun: true } : {}),
          };
        },
      })
      .catch((error: unknown) => {
        // Known no-write refusals published nothing: the caller retains and retries them
        // (smarty-dev#424). Other failures are indeterminate and fail closed at release.
        if (isMeshWriteDeferred(error) || isLockTimeout(error) || error instanceof MeshConsumptionPausedError) throw error;
        this.#releasePublicationFailed = true;
        throw error;
      });
    // Never clear #releasePublicationFailed here: one success cannot reconcile earlier unknown
    // obligations. A retained known no-write outcome settles via #ownedCommands instead.
    // An ACK this host addressed to itself is consumed by its own drain, not its own watcher.
    if (command.replyTo === this.options.hostId) this.#wake();
  }
}
