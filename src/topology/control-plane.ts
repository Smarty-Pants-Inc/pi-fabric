import { copyFabricPrincipal, type FabricPrincipal } from "../fabric-provenance.js";
import { FOLLOW_UP_RUNNING_TASK_MESSAGE, type AgentFollowUpRunningWarning } from "../agents/types.js";
import { createHash, randomUUID } from "node:crypto";
import path from "node:path";
import { mainExecutionCeilingAbortReason, withoutMainExecutionCeiling } from "../async-settlement.js";
import type { FabricActorRunBinding, FabricActorBindingProvenance } from "../actors/types.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../mesh/store.js";
import { MeshBackgroundQueue, MeshBackgroundRetry } from "../core/atomic-write.js";
import { rethrowMeshLockTimeout } from "../core/atomic-write.js";

const CONTROL_TOPIC = "fabric.control.command";
const ACK_TOPIC = "fabric.control.ack";
const CONTROL_SEEN_PREFIX = "topology/control-seen/";
// The participant directory's host records (topology/participant-directory.ts).
const HOST_PREFIX = "topology/hosts/";
const DEFAULT_POLL_MS = 100;
const DEFAULT_ACK_TIMEOUT_MS = 5_000;
// A cancellation owns a separate retry deadline, longer than a production mesh lock wait.
const CANCELLATION_RETENTION_MS = 60_000;
const CONTROL_COMMAND_EXPIRED = "Fabric control command expired";
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

export type FabricControlOperation = "steer" | "followUp" | "stop" | "ask" | "cancel";

export interface FabricControlCommand {
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
  triggerTurn?: boolean;
  binding?: FabricActorRunBinding;
  bindingProvenance?: FabricActorBindingProvenance;
  cancelCommandId?: string;
  requestedAt: number;
  deadlineAt?: number;
}

export interface FabricControlAcceptance {
  warning?: AgentFollowUpRunningWarning;
  accepted: boolean;
  messageId?: string;
  /** Main requested a new turn at admission; absent on older owners or non-Main targets. */
  triggered?: boolean;
  /** Main held a requested wake; includes the provider retry deadline when applicable. */
  reason?: string;
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
  /** The owner proves the handler did not run for this command, so a new command cannot deliver twice. */
  notRun?: true;
}

/** An owner's rejection; `notRun` only when the owner proved the handler did not run. */
class FabricControlRejection extends Error {
  constructor(message: string, readonly notRun: boolean) {
    super(message);
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
      data.operation !== "cancel") ||
    typeof data.replyTo !== "string" ||
    typeof data.requestedAt !== "number" ||
    (data.deadlineAt !== undefined && typeof data.deadlineAt !== "number") ||
    (data.destinationRemoteHost !== undefined && data.destinationRemoteHost !== null &&
      typeof data.destinationRemoteHost !== "string") ||
    (data.operation === "cancel" && typeof data.cancelCommandId !== "string") ||
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
  return { ...data, principal: event.verification === "mesh" || event.verification === "bridge"
    ? copyFabricPrincipal(event.principal) : undefined } as unknown as FabricControlCommand;
};

interface FabricControlSeenRecord {
  format: 1;
  hostId: string;
  commandId: string;
  targetId: string;
  expiresAt: number;
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
    typeof value.expiresAt !== "number"
  ) {
    return undefined;
  }
  return value as unknown as FabricControlSeenRecord;
};

export interface FabricControlPlaneOptions {
  enabled: boolean;
  hostId: string;
  pollMs?: number;
  canConsumeMesh?: () => boolean;
  acknowledgementTimeoutMs?: number;
  /** Remote-link command window; at least 30 s, also used for bridged cancellation. */
  bridgeTimeoutMs?: number;
  /** Fresh, directory-validated mirror ownership, including expired leases. */
  readMirroredOwner?: (
    ownerHostId: string,
    ownerIdentityId: string | undefined,
    targetId: string,
  ) => { remoteHost: string; expiresAt: number } | undefined;
}

export interface FabricControlInput {
  /** Host-only scope snapshot, not a model-facing parameter. */
  principal?: FabricPrincipal | undefined;
  message?: string;
  data?: unknown;
  triggerTurn?: boolean;
  binding?: FabricActorRunBinding;
  bindingProvenance?: FabricActorBindingProvenance;
}

/** Trust owner-default provenance only from a validated member of that actor's root. */
export const controlActorBindingOptions = (
  command: Pick<FabricControlCommand, "binding" | "bindingProvenance">,
  from: MeshIdentity,
  actorRootId: string | undefined,
  senderRootId: string | undefined,
): { overrides?: FabricActorRunBinding; binding?: FabricActorRunBinding } => {
  const provenance = command.bindingProvenance;
  if (provenance) {
    if (provenance.kind !== "owner-defaults" || !actorRootId || provenance.rootId !== actorRootId ||
      (from.id !== actorRootId && senderRootId !== actorRootId)) {
      throw new Error("Invalid actor owner-default binding provenance");
    }
    return { overrides: command.binding ?? {} };
  }
  // An explicit caller view is fixed, even when one or both fields are absent.
  if (command.binding !== undefined) return { binding: command.binding };
  // Preserve legacy unbound own-root requests, but never promote an empty foreign
  // view into the owner's private session defaults.
  return actorRootId && (from.id === actorRootId || senderRootId === actorRootId)
    ? {} : { binding: {} };
};

export interface FabricControlRequestOptions {
  /** Snapshot from the validated participant, never from message/data. */
  routedRemoteHost?: string | null;
  timeoutMs?: number;
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
  targetId: string;
  commandPublished: boolean;
  readonly destinationRemoteHost?: string | null;
  cancellationRequested?: boolean;
  cancellationPublished?: boolean;
  mirroredOwner?: { remoteHost: string; expiresAt: number };
  signal?: AbortSignal;
  onAbort?: () => void;
}

export class FabricControlPlane {
  readonly #pending = new Map<string, PendingControlRequest>();
  readonly #activeCommands = new Map<
    string,
    { controller: AbortController; requesterId: string; targetId: string }
  >();
  readonly #activeHandlers = new Set<Promise<void>>();
  // smarty-dev#424: progress kept across a retried command, since a lock timeout can come
  // between two of its writes. Shared claims this runtime won, by seen key; outcomes of
  // commands that ran but whose acknowledgement is not yet published, by command id. Each
  // entry expires with its command's answer window and is evicted on the next drain.
  readonly #sharedClaims = new Map<string, number>();
  readonly #unpublished = new Map<string, { acceptance: FabricControlAcceptance; expiresAt: number }>();
  readonly #pollMs: number;
  readonly #ackTimeoutMs: number;
  readonly #bridgeTimeoutMs: number;
  #offset: number;
  #lastSequence: number;
  #timer: NodeJS.Timeout | undefined;
  #mirrorWatchdog: NodeJS.Timeout | undefined;
  #polling: Promise<void> | undefined;
  readonly #backgroundPoll = new MeshBackgroundRetry("control claim/ack poll");
  readonly #backgroundNotifications = new MeshBackgroundQueue("control detached ack");
  // Cancellation may become publishable after close, when an admitted command finally commits.
  // Its queue owns that final obligation until success/deadline; idle has no timer or resources.
  readonly #backgroundCancellations = new MeshBackgroundQueue("control cancellation");
  #closed = false;
  #paused = false;
  #releasePublicationFailed = false;
  #handler: FabricControlHandler | undefined;
  #seenCleanupAt = 0;
  #legacySeenCleanupAt = Date.now();
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
    this.#pollMs = Math.max(20, options.pollMs ?? DEFAULT_POLL_MS);
    this.#ackTimeoutMs = Math.max(this.#pollMs * 4, options.acknowledgementTimeoutMs ?? DEFAULT_ACK_TIMEOUT_MS);
    this.#bridgeTimeoutMs = Math.max(MIN_BRIDGE_CONTROL_TIMEOUT_MS,
      Math.min(MAX_CONTROL_TIMEOUT_MS, Math.floor(options.bridgeTimeoutMs ?? MIN_BRIDGE_CONTROL_TIMEOUT_MS)));
    this.#seen = new MeshStore(
      path.join(mesh.root, "control-seen", createHash("sha256").update(options.hostId).digest("hex").slice(0, 32)),
      mesh.maxEventBytes,
      mesh.maxReadEvents,
      { lockProtocol: mesh.lockProtocol },
    );
    // Replay the retained log from its current generation. Durable claims
    // recover unclaimed commands and make interrupted outcomes explicit without re-execution.
    this.#offset = 0;
    this.#lastSequence = 0;
  }

  start(handler: FabricControlHandler): void {
    this.#handler = handler;
    if (!this.options.enabled || this.#timer) return;
    this.#closed = false;
    this.#paused = false;
    this.#timer = setInterval(() => void this.#backgroundPoll.run(() => this.#poll()), this.#pollMs);
    this.#timer.unref();
  }

  async request(
    ownerHostId: string,
    targetId: string,
    operation: FabricControlOperation,
    input: FabricControlInput = {},
    ownerIdentityId = ownerHostId,
    options: FabricControlRequestOptions = {},
  ): Promise<FabricControlResult> {
    // Shared across the bounded retry: first admission may discover a mirror even
    // for an older caller that supplied no routing snapshot.
    const destination = { remoteHost: options.routedRemoteHost };
    const send = () => this.#requestAcceptance(
      ownerHostId,
      targetId,
      operation,
      input,
      ownerIdentityId,
      { ...options, timeoutMs: options.timeoutMs ?? this.#ackTimeoutMs },
      destination,
      true,
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
      sent = await send();
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
    };
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
      input,
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
  ): Promise<{ commandId: string; acceptance: FabricControlAcceptance }> {
    if (!this.options.enabled) {
      throw new Error("Fabric mesh is disabled; cannot control a remote participant");
    }
    if (!ownerHostId.trim()) throw new Error("Remote participant has no execution owner");
    if (options.signal?.aborted) throw new Error(`Remote Fabric request cancelled: ${targetId}`);
    const commandId = randomUUID();
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
    // One budget drives the wire deadline and the sender's ACK timer: mirrored owners
    // need bridge transit time, while native commands retain the local ACK window.
    const timeoutMs = Math.max(
      this.#pollMs * 4,
      typeof destinationRemoteHost === "string" ? this.#bridgeTimeoutMs : 0,
      Math.min(MAX_CONTROL_TIMEOUT_MS, Math.floor(options.timeoutMs ?? this.#ackTimeoutMs)),
    );
    const ackGraceMs = Math.min(MAX_CONTROL_ACK_GRACE_MS, 2 * timeoutMs);
    let pendingRequest: PendingControlRequest;
    const acceptance = new Promise<FabricControlAcceptance>((resolve, reject) => {
      const pending: PendingControlRequest = {
        resolve,
        reject,
        ownerHostId,
        ownerIdentityId,
        targetId,
        commandPublished: false,
        ...(destinationRemoteHost !== undefined ? { destinationRemoteHost } : {}),
        ...(mirroredOwner ? { mirroredOwner: { ...mirroredOwner } } : {}),
      };
      pendingRequest = pending;
      this.#pending.set(commandId, pending);
      if (pending.mirroredOwner) {
        // Capture validated authority first, then arm before publish can wait on the mesh lock.
        // Renewal or a failed directory read cannot extend this sender-local message budget.
        if (messageRequest && (operation === "steer" || operation === "followUp")) {
          // The finite admission budget includes the bridge window and return-leg ACK grace.
          // It is not refreshed by lease renewal, nor reset by a late publish.
          pending.timer = setTimeout(() => this.#timeoutPending(commandId), timeoutMs + ackGraceMs);
          pending.timer.unref();
        }
        this.#startMirrorWatchdog();
      }
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
      const publishing = this.mesh.publish({
        topic: CONTROL_TOPIC,
        kind: operation,
        from: this.identity,
        principal: input.principal,
        signal: operation === "ask" && options.detachOnMainCeiling
          ? withoutMainExecutionCeiling(options.signal) : options.signal,
        to: ownerHostId,
        // Stamped at commit (smarty-dev#816): the owner gets the whole timeout, not what is
        // left after this sender waited for the mesh lock (2-3 s under load).
        data: (committedAt: number): FabricControlCommand => ({
          version: 1,
          commandId,
          targetId,
          operation,
          replyTo: this.options.hostId,
          ...(destinationRemoteHost !== undefined ? { destinationRemoteHost } : {}),
          ...(input.message !== undefined ? { message: input.message } : {}),
          ...(input.data !== undefined ? { data: input.data } : {}),
          ...(input.triggerTurn !== undefined ? { triggerTurn: input.triggerTurn } : {}),
          ...(input.binding !== undefined ? { binding: input.binding } : {}),
          ...(input.bindingProvenance !== undefined ? { bindingProvenance: input.bindingProvenance } : {}),
          requestedAt: committedAt,
          deadlineAt: committedAt + timeoutMs,
        }),
      }).then(() => {
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
        const error = acknowledged.error || "Remote Fabric owner rejected command for " + targetId;
        throw new FabricControlRejection(
          acknowledged.notRun === true && error === CONTROL_COMMAND_EXPIRED
            ? `${error}; not delivered, safe to resend.`
            : error,
          acknowledged.notRun === true,
        );
      }
      return { commandId, acceptance: acknowledged };
    } catch (error) {
      const cancelled = this.#clearPending(commandId);
      if (cancelled) void this.#publishCancellation(commandId, cancelled);
      throw error;
    }
  }

  #timeoutPending(commandId: string): void {
    const timedOut = this.#clearPending(commandId);
    if (!timedOut) return;
    void this.#publishCancellation(commandId, timedOut);
    // Neither a live lease nor a missing ACK proves the handler did not run. Never replay.
    timedOut.reject(new Error(
      (timedOut.mirroredOwner
        ? `Fabric mesh bridge to remote host ${timedOut.mirroredOwner.remoteHost} is not responding for ${timedOut.targetId}; `
        : `Timed out waiting for the remote Fabric owner to acknowledge ${timedOut.targetId}; `) +
        "the outcome is unknown and it may still be delivered, so a retry can deliver it twice.",
    ));
  }

  #clearPending(commandId: string): PendingControlRequest | undefined {
    const pending = this.#pending.get(commandId);
    if (!pending) return undefined;
    clearTimeout(pending.timer);
    if (pending.signal && pending.onAbort) {
      pending.signal.removeEventListener("abort", pending.onAbort);
    }
    this.#pending.delete(commandId);
    if (this.#mirrorWatchdog && ![...this.#pending.values()].some((request) => request.mirroredOwner)) {
      clearInterval(this.#mirrorWatchdog);
      this.#mirrorWatchdog = undefined;
    }
    return pending;
  }

  #startMirrorWatchdog(): void {
    if (this.#mirrorWatchdog) return;
    // Independent of the async mesh drain: a locked write must not hide a lease lapse.
    this.#mirrorWatchdog = setInterval(() => {
      for (const [commandId, pending] of this.#pending) {
        const captured = pending.mirroredOwner;
        if (!captured) continue;
        let current: PendingControlRequest["mirroredOwner"];
        try {
          current = this.options.readMirroredOwner?.(pending.ownerHostId, pending.ownerIdentityId, pending.targetId);
        } catch {
          // A failed read is not evidence of a lapse. The independent request timer stays armed.
          continue;
        }
        // Never adopt a different link's label. Missing ownership cannot renew this lease.
        if (current?.remoteHost === captured.remoteHost) captured.expiresAt = current.expiresAt;
        if (captured.expiresAt > Date.now()) continue;
        const lapsed = this.#clearPending(commandId);
        if (!lapsed) continue;
        void this.#publishCancellation(commandId, lapsed);
        lapsed.reject(new Error(
          `Fabric lease mirrored from remote host ${captured.remoteHost} lapsed for ${pending.targetId}; ` +
            "the mesh bridge is down or the owner is unavailable; " +
            "the outcome is unknown and it may still be delivered, so a retry can deliver it twice.",
        ));
      }
    }, this.#pollMs);
    this.#mirrorWatchdog.unref();
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
        ...(pending.destinationRemoteHost !== undefined
          ? { destinationRemoteHost: pending.destinationRemoteHost } : {}),
        requestedAt: committedAt,
        deadlineAt: committedAt + Math.max(this.#ackTimeoutMs,
          typeof pending.destinationRemoteHost === "string" ? this.#bridgeTimeoutMs : 0),
      }),
    };
    await this.#backgroundCancellations.enqueue(() => {
      if (Date.now() <= expiresAt) return this.mesh.publish(cancellation);
      return undefined;
    });
  }

  /** Reload: leave new commands unclaimed in the durable mesh log for the next runtime. */
  pause(): void {
    this.#paused = true;
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = undefined;
  }

  resume(): void {
    if (this.#closed || !this.#paused) return;
    this.#paused = false;
    this.#timer = setInterval(() => void this.#backgroundPoll.run(() => this.#poll()), this.#pollMs);
    this.#timer.unref();
    void this.#backgroundPoll.run(() => this.#poll());
  }

  /** Join through outcome and ACK publication, not merely the host admission counter. */
  async checkpointForRelease(): Promise<void> {
    if (!this.#paused) throw new Error("Control release gate is not paused");
    await this.#polling;
    await Promise.all([...this.#activeHandlers]);
    await this.#backgroundNotifications.checkpointForRelease();
    await this.#backgroundCancellations.checkpointForRelease();
    if (this.#activeCommands.size || this.#pending.size || this.#unpublished.size || this.#releasePublicationFailed) {
      throw new Error("Control release has unsettled publication obligations");
    }
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = undefined;
    await this.#polling?.catch(() => undefined);
    if (!this.#paused) await this.#drain().catch(() => undefined);
    this.#closed = true;
    this.#sharedClaims.clear();
    this.#unpublished.clear();
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
    await this.#backgroundNotifications.close();
    this.#handler = undefined;
  }

  async #poll(): Promise<void> {
    if (this.#closed || this.#paused || !this.options.enabled || this.options.canConsumeMesh?.() === false) return;
    if (this.#polling) return this.#polling;
    const operation = this.#drain();
    this.#polling = operation;
    try {
      await operation;
    } finally {
      if (this.#polling === operation) this.#polling = undefined;
    }
  }

  async #drain(): Promise<void> {
    const now = Date.now();
    for (const [key, expiresAt] of this.#sharedClaims) if (expiresAt < now) this.#sharedClaims.delete(key);
    for (const [id, kept] of this.#unpublished) if (kept.expiresAt < now) this.#unpublished.delete(id);
    while (this.options.canConsumeMesh?.() !== false) {
      const tail = this.mesh.tail(this.#offset, 100);
      for (const event of tail.events) {
        if (this.#paused || this.options.canConsumeMesh?.() === false) return;
        if (event.sequence <= this.#lastSequence) continue;
        // An event is consumed only once handled: a command that hit a lock timeout throws,
        // and the next poll reads this page again from it (smarty-dev#424). Each retry is
        // bounded by the command's deadline plus the acknowledgement grace.
        if (event.to === this.options.hostId) {
          if (event.topic === ACK_TOPIC) this.#acceptAcknowledgement(event);
          else if (event.topic === CONTROL_TOPIC) await this.#acceptCommand(event);
        }
        if (this.options.canConsumeMesh?.() === false) return;
        this.#lastSequence = event.sequence;
      }
      this.#offset = tail.nextOffset;
      if (tail.events.length < 100) break;
    }
    if (this.options.canConsumeMesh?.() === false) return;
    await this.#cleanupSeen(Date.now()).catch(rethrowMeshLockTimeout);
  }

  #acceptAcknowledgement(event: MeshEvent): void {
    if (!isObject(event.data) || typeof event.data.commandId !== "string") return;
    const pending = this.#pending.get(event.data.commandId);
    if (
      !pending ||
      event.data.version !== 1 ||
      event.data.targetId !== pending.targetId ||
      event.from.id !== pending.ownerIdentityId ||
      // A validated mirror's answer authority survives record withdrawal/replacement.
      // Known native requests require an unstamped ACK, regardless of later metadata.
      // Only unbound legacy requests use the mutable metadata selector.
      !(pending.mirroredOwner
        ? isObject(event.data.bridge) && event.data.bridge.from === pending.mirroredOwner.remoteHost
        : pending.destinationRemoteHost === null
          ? !Object.prototype.hasOwnProperty.call(event.data, "bridge")
          : this.#bridgeMatches(pending.ownerHostId, event.data))
    ) {
      return;
    }
    this.#clearPending(event.data.commandId);
    pending.resolve({
      accepted: event.data.accepted === true,
      ...(typeof event.data.messageId === "string" ? { messageId: event.data.messageId } : {}),
      ...queueDepthOf(event.data),
      ...coalescedOf(event.data),
      ...runningTaskWarningOf(event.data, pending.targetId),
      ...triggeredOf(event.data),
      ...(Object.prototype.hasOwnProperty.call(event.data, "result")
        ? { result: event.data.result }
        : {}),
      ...(typeof event.data.error === "string" ? { error: event.data.error } : {}),
      ...(event.data.accepted !== true && event.data.notRun === true ? { notRun: true as const } : {}),
    });
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
    const command = commandFromEvent(event);
    if (!command) return;
    if (command.operation === "cancel") {
      this.#acceptCancellation(command, event.from);
      return;
    }
    const now = Date.now();
    const deadlineAt = Math.min(
      command.deadlineAt ?? command.requestedAt + this.#ackTimeoutMs,
      command.requestedAt + MAX_CONTROL_TIMEOUT_MS,
    );
    const key = controlSeenKey(this.options.hostId, command.commandId);
    const answerable = now <= deadlineAt + MAX_CONTROL_ACK_GRACE_MS + this.#pollMs * 4;
    const ran = this.#unpublished.get(command.commandId);
    if (ran) {
      // The command ran, and only its acknowledgement failed: send the real outcome.
      if (answerable) await this.#publishAcknowledgement(command, ran.acceptance);
      this.#unpublished.delete(command.commandId);
      this.#sharedClaims.delete(key);
      return;
    }
    if (now > deadlineAt || command.requestedAt - now > this.#ackTimeoutMs) {
      this.#sharedClaims.delete(key);
      // A sender waits at most MAX_CONTROL_ACK_GRACE_MS past the deadline. An older command
      // is history: a restarting owner replays the retained log from its start, and
      // answering every past command added one locked publish each, thousands per
      // relaunch wave (smarty-dev#367). Only a sender that may still wait gets an answer.
      if (!answerable) return;
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
      if (local) acceptance = local.acceptance ?? indeterminate;
      else {
        const expired = { accepted: false, error: CONTROL_COMMAND_EXPIRED, notRun: true } as const;
        try {
          await this.mesh.put({
            key,
            value: {
              format: 1,
              hostId: this.options.hostId,
              commandId: command.commandId,
              targetId: command.targetId,
              expiresAt: Math.max(deadlineAt, now) + this.#ackTimeoutMs,
              ...(command.deadlineAt !== undefined ? { explicitDeadline: true } : {}),
              acceptance: expired,
            } satisfies FabricControlSeenRecord,
            identity: this.identity,
            ifVersion: 0,
          });
          acceptance = expired;
        } catch (error) {
          if (isLockTimeout(error)) throw error;
          acceptance = ownRecord(controlSeenRecord(this.mesh.get(key, { fresh: true })?.value))?.acceptance ??
            indeterminate;
        }
      }
      await this.#publishAcknowledgement(command, acceptance);
      return;
    }

    // A shared claim this runtime already won is its own, not a duplicate.
    const duplicate = this.#sharedClaims.has(key)
      ? controlSeenRecord(this.#seen.get(key)?.value)
      : this.#seenRecord(key);
    if (duplicate) {
      if (
        duplicate.hostId === this.options.hostId &&
        duplicate.commandId === command.commandId &&
        duplicate.targetId === command.targetId
      ) {
        await this.#publishAcknowledgement(
          command,
          duplicate.acceptance ?? {
            accepted: false,
            error: "Fabric control outcome is indeterminate after owner restart",
          },
        );
      }
      return;
    }

    let claim;
    try {
      // One claim authority across versions (smarty-dev#643, phase 1): runtimes before this
      // change claim only the shared key, so this runtime claims it first and runs the command
      // only when that claim wins too. The outcome is kept only in this host's own store.
      if (!this.#sharedClaims.has(key)) await this.mesh.put({
        key,
        value: {
          format: 1,
          hostId: this.options.hostId,
          commandId: command.commandId,
          targetId: command.targetId,
          expiresAt: deadlineAt + this.#ackTimeoutMs,
          ...(command.deadlineAt !== undefined ? { explicitDeadline: true } : {}),
        } satisfies FabricControlSeenRecord,
        identity: this.identity,
        ifVersion: 0,
      });
      this.#sharedClaims.set(key, deadlineAt + MAX_CONTROL_ACK_GRACE_MS + this.#pollMs * 4);
      claim = await this.#seen.put({
        key,
        value: {
          format: 1,
          hostId: this.options.hostId,
          commandId: command.commandId,
          targetId: command.targetId,
          expiresAt: deadlineAt + this.#ackTimeoutMs,
          sequence: event.sequence,
        } satisfies FabricControlSeenRecord,
        identity: this.identity,
        ifVersion: 0,
      });
    } catch (error) {
      if (isLockTimeout(error)) throw error;
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
        );
      }
      return;
    }

    this.#sharedClaims.delete(key);
    const execution = this.#executeClaimedCommand(
      command,
      event.from,
      key,
      claim.version,
      deadlineAt,
      event.sequence,
      event.verification,
    );
    if (command.operation === "ask") {
      this.#activeHandlers.add(execution);
      void execution.finally(() => this.#activeHandlers.delete(execution)).catch(() => undefined);
      return;
    }
    await execution;
  }

  #acceptCancellation(command: FabricControlCommand, from: MeshIdentity): void {
    if (!command.cancelCommandId) return;
    const active = this.#activeCommands.get(command.cancelCommandId);
    if (
      active &&
      active.requesterId === from.id &&
      active.targetId === command.targetId
    ) {
      active.controller.abort();
    }
  }

  async #executeClaimedCommand(
    command: FabricControlCommand,
    from: MeshIdentity,
    key: string,
    claimVersion: number,
    deadlineAt: number,
    sequence: number,
    verification: MeshEvent["verification"],
  ): Promise<void> {
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
        if (Date.now() > deadlineAt) acceptance = { accepted: false, error: CONTROL_COMMAND_EXPIRED, notRun: true };
        else acceptance = this.#handler
          ? await this.#handler(command, from, controller.signal, verification)
          : { accepted: false, error: "Fabric owner has no control handler" };
      } catch (error) {
        acceptance = {
          accepted: false,
          error: error instanceof Error ? error.message : String(error),
        };
      }
      acceptance = this.#boundedAcceptance(acceptance);
      const saveOutcome = () => this.#seen.put({
          key,
          value: {
            format: 1,
            hostId: this.options.hostId,
            commandId: command.commandId,
            targetId: command.targetId,
            expiresAt: Math.max(deadlineAt, Date.now()) + this.#ackTimeoutMs,
            sequence,
            acceptance,
          } satisfies FabricControlSeenRecord,
          identity: this.identity,
          ifVersion: claimVersion,
        });
      try {
        await saveOutcome();
      } catch (error) {
        this.#releasePublicationFailed = true;
        // A conflict belongs to another owner; a timeout wrote nothing. Preserve the actual
        // result under the original version fence and retry it without running the handler.
        if (!isLockTimeout(error)) return;
        void this.#backgroundNotifications.retry(() => {
          if (Date.now() <= deadlineAt + MAX_CONTROL_ACK_GRACE_MS + this.#pollMs * 4) return saveOutcome();
          return undefined;
        }, error);
      }
      if (command.operation === "ask") {
        // Detached asks have already left the poll cursor. Retain just their outcome, never
        // execute the handler again, and retry the ACK on the owned notification tick.
        await this.#backgroundNotifications.enqueue(() => {
          if (Date.now() <= deadlineAt + MAX_CONTROL_ACK_GRACE_MS + this.#pollMs * 4) {
            return this.#publishAcknowledgement(command, acceptance);
          }
          return undefined;
        });
        return;
      }
      try {
        await this.#publishAcknowledgement(command, acceptance);
      } catch (error) {
        this.#releasePublicationFailed = true;
        // Non-detached commands remain at the poll cursor. Keep the actual outcome for
        // its next pass, rather than executing a handler twice after an ACK lock timeout.
        this.#unpublished.set(command.commandId, {
          acceptance,
          expiresAt: deadlineAt + MAX_CONTROL_ACK_GRACE_MS + this.#pollMs * 4,
        });
        throw error;
      }
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
    if (now - this.#seenCleanupAt < this.#ackTimeoutMs) return;
    this.#seenCleanupAt = now;
    // A restarting owner replays the retained log from its start, so a record stays while its
    // command is still in the log: past its expiry, and below the log's oldest sequence.
    const oldest = this.mesh.oldestSequence();
    if (oldest !== undefined) {
      const stale = this.#seen.listAll(CONTROL_SEEN_PREFIX).filter((entry) => {
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
  ): Promise<void> {
    await this.mesh
      .publish({
        topic: ACK_TOPIC,
        kind: acceptance.accepted ? "accepted" : "rejected",
        from: this.identity,
        to: command.replyTo,
        data: {
          version: 1,
          commandId: command.commandId,
          targetId: command.targetId,
          accepted: acceptance.accepted,
          ...(acceptance.messageId ? { messageId: acceptance.messageId } : {}),
          ...queueDepthOf(acceptance as unknown as Record<string, unknown>),
          ...coalescedOf(acceptance as unknown as Record<string, unknown>),
          ...runningTaskWarningOf(acceptance as unknown as Record<string, unknown>, command.targetId),
          ...triggeredOf(acceptance as unknown as Record<string, unknown>),
          ...(Object.prototype.hasOwnProperty.call(acceptance, "result")
            ? { result: acceptance.result }
            : {}),
          ...(acceptance.error ? { error: acceptance.error } : {}),
          ...(!acceptance.accepted && acceptance.notRun ? { notRun: true } : {}),
        },
      })
      .catch((error: unknown) => {
        // A lock timeout published nothing: the caller retries it (smarty-dev#424).
        if (isLockTimeout(error)) throw error;
      });
  }
}
