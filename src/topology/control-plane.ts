import { appendDeliveryOutcome, type DeliverySend } from "../mesh/delivery-outcomes.js";
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
// Source-local recovery only after an authenticated relay refusal. The UDS relay's
// fixed 4 s receipt window permits at most this 8 s observation window; no idle timer.
// Keep this constant local: importing the optional UDS runtime would widen startup.
const RELAY_ACK_RECOVERY_TTL_MS = 8_000;
const RELAY_ACK_RECOVERY_LIMIT = 256;
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
  console.warn(`[pi-fabric] Unfenced legacy control delivery count=${count} operation=${operation} target=${JSON.stringify(targetId)}`);
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
  errorCode?: typeof CONTROL_STALE_INCARNATION | FabricInterruptErrorCode;
  /** The owner proves the handler did not run for this command, so a new command cannot deliver twice. */
  notRun?: true;
}

/** An owner's rejection; `notRun` only when the owner proved the handler did not run. */
class FabricControlRejection extends Error {
  /** Marks an owner's remote rejection; the router records no local refusal outcome for it. */
  readonly rejection = "FABRIC_CONTROL_REJECTED";
  constructor(message: string, readonly notRun: boolean, readonly code?: FabricInterruptErrorCode) {
    super(message);
    if (code) this.name = code === "FABRIC_INTERRUPT_NOT_AUTHORIZED"
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
  const { sender: _forged, ...command } = data;
  const wakeCause = data.operation === "steer" || data.operation === "followUp"
    ? fabricWakeCause(event.from, data.operation, event.topic, event.id) : undefined;
  return withFabricWakeAdmission({ ...command, eventId: event.id, wakeCause, principal: event.verification === "mesh" || event.verification === "bridge"
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
  "ownerHostId" | "ownerIdentityId" | "ownerIncarnation" | "targetId" | "destinationRemoteHost">> & {
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
  acceptance?: FabricControlAcceptance;
  saved?: boolean;
  running?: boolean;
  /** Only saved outcomes enter the serial ACK worker; retain the original wire authority. */
  acknowledgement?: { command: FabricControlCommand; ownerIncarnation: string | null };
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
  // Insertion order is LRU, shared by the authority-only and validated-ACK entries.
  readonly #relayAcknowledgements = new Map<string, RetainedRelayAcknowledgement>();

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
  // Retain progress across lock timeout AND lease loss. Shared claims owned by
  // this runtime precede local claim admission; owned commands retain the exact
  // outcome until its sequence and ACK commit, never replaying a completed handler.
  readonly #sharedClaims = new Map<string, number>();
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
    const results = await store.writeBatch({ identity: this.identity, ops: [], prepare: () => {
      if (admission) this.#assertAdmission();
      else assertMeshConsumption(this.options.canConsumeMesh);
      return [{ kind: "put", ...input }];
    } });
    return { version: results[0]!.version };
  }
  readonly #pollMs: number;
  readonly #ackTimeoutMs: number;
  readonly #bridgeTimeoutMs: number;
  #offset: number;
  #lastSequence: number;
  // Drains run at start, on mesh event-log notifications and after this process's own
  // publishes. Idle has no timer: #retryTimer exists only for retained work (a failed or
  // deferred drain's backoff, a gated drain, a retained owned outcome, or a shared claim's
  // real expiry), #rearmTimer only for a failed watcher's bounded re-arm. The shared
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
  #wakeScheduled = false;
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
   * across reloads. While runtimes before this change may run, each claim is also made in the
   * shared state (see #acceptCommand).
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
    this.#seen = new MeshStore(
      path.join(mesh.root, "control-seen", createHash("sha256").update(options.hostId).digest("hex").slice(0, 32)),
      mesh.maxEventBytes,
      mesh.maxReadEvents,
      // control-seen stays on the file backend (smarty-dev#6477 R20).
      { lockProtocol: mesh.lockProtocol, stateBackend: "file" },
    );
    // Replay the retained log from its current generation. Durable claims
    // recover unclaimed commands and make interrupted outcomes explicit without re-execution.
    this.#offset = 0;
    this.#lastSequence = 0;
  }

  start(handler: FabricControlHandler): void {
    this.#handler = handler;
    if (!this.options.enabled || this.#started) return;
    this.#started = true;
    this.#closed = false;
    this.#paused = false;
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
        !error.notRun
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
  ): Promise<{ commandId: string; acceptance: FabricControlAcceptance }> {
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
    const commandId = createHash("sha256")
      .update(JSON.stringify([this.options.hostId, ownerHostId, ownerIdentityId, targetId, operation, idempotencyKey, notRunAttempt]))
      .digest("hex");
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
      const publishOnce = () => this.mesh.publish({
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
    if (timedOut.mirroredOwner || typeof timedOut.destinationRemoteHost === "string" || !timedOut.commandPublished) {
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
   * record (the same record the owner writes for a command it reads past its deadline). Both the
   * owner's admission and its duplicate check read this claim before running the handler, so a
   * win proves the message was never and will never be delivered. A lost race means the owner
   * holds it, and the outcome stays unknown.
   */
  async #withdrawUnclaimed(commandId: string, pending: PendingControlRequest): Promise<boolean> {
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
      // The owner may itself have recorded the expiry first.
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
      const event = await this.mesh.publish(cancellation);
      this.#wake();
      return event;
    });
  }

  /** Reload: leave commands queued; the next activation fences execution-bound addresses. */
  pause(_strict = false): void {
    if (!this.#paused) this.#releaseEpoch++;
    this.#paused = true;
    // Pending work stays #dirty; resume() drains it. A paused plane arms no retry.
    clearTimeout(this.#retryTimer);
    this.#retryTimer = undefined;
  }

  resume(): void {
    if (this.#closed || !this.#paused) return;
    this.#paused = false;
    this.#releaseEpoch++;
    this.#wake();
  }

  /** Join through outcome and ACK publication, not merely the host admission counter. */
  async checkpointForRelease(valid?: () => boolean): Promise<void> {
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
    if (this.#activeCommands.size || this.#pending.size || this.#resendWaits.size || this.#sharedClaims.size || this.#ownedCommands.size || this.#releasePublicationFailed) {
      throw new Error("Control release has unsettled publication obligations");
    }
    const state = JSON.stringify(this.#readReleaseState());
    check();
    this.#releaseCheckpoint = { epoch, state };
  }

  #readReleaseState(): unknown {
    const localOutcomes = this.#seen.listAll(CONTROL_SEEN_PREFIX, { fresh: true }).sort((a, b) => a.key.localeCompare(b.key));
    const sharedClaims = this.mesh.listAll(CONTROL_SEEN_PREFIX, { fresh: true })
      .filter(entry => controlSeenRecord(entry.value)?.hostId === this.options.hostId).sort((a, b) => a.key.localeCompare(b.key));
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
    const outcomes = new Map(localOutcomes.map(entry => [entry.key, controlSeenRecord(entry.value)]));
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
    for (const entry of sharedClaims) {
      const record = controlSeenRecord(entry.value)!;
      if (!record.acceptance && !outcomes.get(entry.key)?.acceptance) throw new Error("Control shared claim outcome disk readback is unconfirmed");
    }
    return { localOutcomes, sharedClaims, acknowledgements, consumedSequence: this.#lastSequence };
  }

  releaseCheckpointReceipt(): unknown {
    const saved = this.#releaseCheckpoint;
    if (!this.#paused || !saved || saved.epoch !== this.#releaseEpoch || this.#polling || this.#activeCommands.size ||
        this.#activeHandlers.size || this.#pending.size || this.#resendWaits.size || this.#sharedClaims.size || this.#ownedCommands.size || this.#releasePublicationFailed) {
      throw new Error("Control release checkpoint receipt is unavailable or unsettled");
    }
    const state = this.#readReleaseState();
    if (JSON.stringify(state) !== saved.state) throw new Error("Control release checkpoint disk readback changed");
    return { admissionHeld: true, state, receiptScope: "persisted claims/outcomes and correlated ACK log, not UI quiet" };
  }

  async close(): Promise<void> {
    if (this.#closed) return;
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
    this.#sharedClaims.clear();
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
      if (!this.#ownedCommands.size && !this.#sharedClaims.size) this.#backgroundPoll.success();
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
      try { console.warn(`[pi-fabric] control plane: mesh state change subscription failed (${error instanceof Error ? error.message : String(error)}); relying on mesh directory notifications.`); } catch {}
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
        console.warn(`[pi-fabric] control plane: mesh notifications failed for ${this.mesh.root} (${cause}); ` +
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
    if (this.#wakeScheduled) return;
    this.#wakeScheduled = true;
    queueMicrotask(() => {
      this.#wakeScheduled = false;
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
    const now = Date.now();
    const legacyDeadline = this.#legacySeenPending
      ? this.#legacySeenCleanupAt + LEGACY_SEEN_CLEANUP_MS : Infinity;
    // A spent claim may outlive its compacted input; keep custody but reconcile it on cadence.
    const claimDeadlines = [...this.#sharedClaims.values()].map((deadline) =>
      deadline > now ? deadline : now + LEGACY_SEEN_CLEANUP_MS);
    // An overdue deadline must wait until cleanup can act, not spin through its throttle.
    const expiresAt = Math.max(this.#seenCleanupAt + this.#ackTimeoutMs,
      Math.min(legacyDeadline, ...claimDeadlines));
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
    while (this.options.canConsumeMesh?.() !== false) {
      if (this.#acknowledgementsFull()) {
        this.#ackCapacityBlocked = true;
        return false; // stop before fetching the next page; ACK settlement wakes the reader
      }
      const tail = this.mesh.tail(this.#offset, 100);
      for (const event of tail.events) {
        if (this.#paused || this.options.canConsumeMesh?.() === false) return false;
        if (event.sequence <= this.#lastSequence) continue;
        // An event is consumed only once handled: a command that hit a lock timeout throws,
        // and the next poll reads this page again from it (smarty-dev#424). Each retry is
        // bounded by the command's deadline plus the acknowledgement grace.
        if (event.to === this.options.hostId) {
          if (event.topic === ACK_TOPIC) {
            // A hub speaks only for relay infrastructure, never for the destination owner.
            const envelope = ownDataRecord(event);
            const data = envelope && ownDataRecord(envelope.data);
            if (data?.errorCode === RELAY_DESTINATION_UNAVAILABLE || data?.errorCode === FABRIC_IDEMPOTENCY_KEY_REUSED) this.#acceptRelayRefusal(event);
            else if (data) this.#acceptAcknowledgement(event);
          } else if (event.topic === CONTROL_TOPIC) {
            if (this.#acknowledgementsFull()) {
              const command = commandFromEvent(event);
              if (command && command.operation !== "cancel" && !this.#ownedCommands.has(controlSeenKey(this.options.hostId, command.commandId))) {
                this.#ackCapacityBlocked = true;
                return false; // leave this new command unread; never drop an ACK obligation
              }
            }
            await this.#acceptCommand(event);
          }
        }
        if (this.options.canConsumeMesh?.() === false) return false;
        this.#lastSequence = event.sequence;
      }
      this.#offset = tail.nextOffset;
      if (tail.events.length < 100) break;
    }
    if (this.#paused || this.options.canConsumeMesh?.() === false) return false;
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
      ...(data.accepted !== true && data.notRun === true ? { notRun: true as const } : {}),
    };
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
    if (now > deadlineAt + MAX_CONTROL_ACK_GRACE_MS + this.#pollMs * 4 && !lateDelivery && !this.#sharedClaims.has(key)) return;
    // Dedupe precedes the new-admission fence: completed earlier outcomes remain observable,
    // but their ACK names the claiming activation, never this publisher's new activation.
    const duplicate = this.#sharedClaims.has(ownedKey)
      ? controlSeenRecord(this.#seen.get(ownedKey)?.value) : this.#seenRecord(ownedKey);
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
    if ((now > deadlineAt || command.requestedAt - now > this.#ackTimeoutMs) && !this.#sharedClaims.has(key)) {
      // A sender waits at most MAX_CONTROL_ACK_GRACE_MS past the deadline. An older command
      // is history: a restarting owner replays the retained log from its start, and
      // answering every past command added one locked publish each, thousands per
      // relaunch wave (smarty-dev#367). Only a sender that may still wait gets an answer.
      // An owner restarted past the deadline may have run it before, and a runtime before
      // smarty-dev#643 claims only the shared key. No read proves it did not run (a read can be
      // cached, and a claim can land after it): this runtime wins the shared claim first, with
      // the same create-only put as admission, and records the expiry in it. Then no runtime,
      // new or old, can claim or run it. Otherwise the claim holder's record answers, never
      // notRun (review/astra F1 on #121). A runtime that admitted it in time and claims it now
      // expires it before the handler (see #executeClaimedCommand).
      const ownRecord = (record: FabricControlSeenRecord | undefined) =>
        record?.hostId === this.options.hostId && record.commandId === command.commandId &&
          record.targetId === command.targetId ? record : undefined;
      const indeterminate = { accepted: false, error: "Fabric control outcome is indeterminate after owner restart" };
      const local = ownRecord(controlSeenRecord(this.#seen.get(key)?.value));
      let acceptance: FabricControlAcceptance;
      let outcomeIncarnation: string | null = this.incarnation;
      if (local) {
        acceptance = local.acceptance ?? indeterminate;
        outcomeIncarnation = local.ownerIncarnation ?? null;
      }
      else {
        const expired = { accepted: false, error: CONTROL_COMMAND_EXPIRED, notRun: true } as const;
        try {
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
              acceptance: expired,
            } satisfies FabricControlSeenRecord,
            identity: this.identity,
            ifVersion: 0,
          }, true);
          acceptance = expired;
        } catch (error) {
          if (isMeshWriteDeferred(error) || isLockTimeout(error) || error instanceof MeshConsumptionPausedError) throw error;
          const raced = ownRecord(controlSeenRecord(this.mesh.get(key, { fresh: true })?.value));
          acceptance = raced?.acceptance ?? indeterminate;
          outcomeIncarnation = raced?.ownerIncarnation ?? null;
        }
      }
      await this.#publishAcknowledgement(command, acceptance, outcomeIncarnation);
      return;
    }

    let claim;
    try {
      // One claim authority across versions (smarty-dev#643, phase 1): runtimes before this
      // change claim only the shared key, so this runtime claims it first and runs the command
      // only when that claim wins too. The outcome is kept only in this host's own store.
      if (!this.#sharedClaims.has(key)) await this.#putFenced(this.mesh, {
        key,
        value: {
          format: 1,
          hostId: this.options.hostId,
          commandId: command.commandId,
          targetId: command.targetId,
          ownerIncarnation: this.incarnation,
          expiresAt: deadlineAt + this.#ackTimeoutMs,
          ...(command.deadlineAt !== undefined ? { explicitDeadline: true } : {}),
        } satisfies FabricControlSeenRecord,
        identity: this.identity,
        ifVersion: 0,
      }, true);
      // Retain a committed shared claim even if PREPARE landed during the await.
      this.#sharedClaims.set(key, deadlineAt + MAX_CONTROL_ACK_GRACE_MS + this.#pollMs * 4);
      this.#assertAdmission();
      claim = await this.#putFenced(this.#seen, {
        key,
        value: {
          format: 1,
          hostId: this.options.hostId,
          commandId: command.commandId,
          targetId: command.targetId,
          ownerIncarnation: this.incarnation,
          expiresAt: deadlineAt + this.#ackTimeoutMs,
        } satisfies FabricControlSeenRecord,
        identity: this.identity,
        ifVersion: 0,
      }, true);
    } catch (error) {
      if (isMeshWriteDeferred(error) || isLockTimeout(error) || error instanceof MeshConsumptionPausedError) throw error;
      this.#sharedClaims.delete(key);
      const raced = this.#seenRecord(key);
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

    this.#sharedClaims.delete(key);
    const admitted = { event, claimVersion: claim.version };
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
      else console.warn(`[pi-fabric] ${message}`);
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
        else if (Date.now() > deadlineAt) acceptance = { accepted: false, error: CONTROL_COMMAND_EXPIRED, notRun: true };
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
  #seenRecord(key: string): FabricControlSeenRecord | undefined {
    return controlSeenRecord(this.#seen.get(key)?.value) ?? controlSeenRecord(this.mesh.get(key)?.value);
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
    if (oldest !== undefined) {
      const stale = this.#seen.listAll(CONTROL_SEEN_PREFIX).filter((entry) => {
        if (this.#ownedCommands.has(entry.key)) return false;
        const record = controlSeenRecord(entry.value);
        return !record || (record.expiresAt < now && record.sequence !== undefined && record.sequence < oldest);
      });
      if (stale.length > 0) {
        await this.#seen.writeBatch({
          identity: this.identity,
          ops: stale.map((entry) => ({ kind: "delete" as const, key: entry.key, ifVersion: entry.version, onConflict: "skip" as const })),
        });
      }
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
    // One write for the whole sweep, each delete fenced to the version it saw.
    await this.mesh.writeBatch({
      identity: this.identity,
      ops: dead.map(({ entry }) => ({ kind: "delete" as const, key: entry.key, ifVersion: entry.version, onConflict: "skip" as const })),
    });
  }

  async #publishAcknowledgement(
    command: FabricControlCommand,
    acceptance: FabricControlAcceptance,
    ownerIncarnation: string | null = this.incarnation,
    // null explicitly attests that this command arrived without an epoch.
    staleIncarnation?: string | null,
  ): Promise<void> {
    await this.mesh.publish({
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
