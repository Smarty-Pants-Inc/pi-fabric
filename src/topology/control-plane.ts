import { createHash, randomUUID } from "node:crypto";
import path from "node:path";
import type { FabricActorRunBinding } from "../actors/types.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../mesh/store.js";

const CONTROL_TOPIC = "fabric.control.command";
const ACK_TOPIC = "fabric.control.ack";
const CONTROL_SEEN_PREFIX = "topology/control-seen/";
const DEFAULT_POLL_MS = 100;
const DEFAULT_ACK_TIMEOUT_MS = 5_000;
const DEFAULT_RESULT_TIMEOUT_MS = 60 * 60 * 1_000;
const MAX_CONTROL_TIMEOUT_MS = 24 * 60 * 60 * 1_000 + 60_000;
// The sender keeps waiting this long past the command deadline. The owner admits a
// command only before its deadline, but its acknowledgement can queue for the mesh lock:
// without this grace a delivered command was reported as timed out and then retried
// (smarty-dev#367). A command not admitted by the deadline is acknowledged as expired.
const MAX_CONTROL_ACK_GRACE_MS = 15_000;
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
  version: 1;
  commandId: string;
  targetId: string;
  operation: FabricControlOperation;
  replyTo: string;
  message?: string;
  data?: unknown;
  triggerTurn?: boolean;
  binding?: FabricActorRunBinding;
  cancelCommandId?: string;
  requestedAt: number;
  deadlineAt?: number;
}

export interface FabricControlAcceptance {
  accepted: boolean;
  messageId?: string;
  result?: unknown;
  error?: string;
}

export interface FabricControlResult {
  queued: true;
  messageId: string;
  routed: "mesh";
  acknowledged: true;
}

export type FabricControlHandler = (
  command: FabricControlCommand,
  from: MeshIdentity,
  signal: AbortSignal,
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
    (data.operation === "cancel" && typeof data.cancelCommandId !== "string") ||
    (data.binding !== undefined &&
      (!isObject(data.binding) ||
        (data.binding.model !== undefined && typeof data.binding.model !== "string") ||
        (data.binding.thinking !== undefined && typeof data.binding.thinking !== "string")))
  ) {
    return undefined;
  }
  return data as unknown as FabricControlCommand;
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
  acknowledgementTimeoutMs?: number;
}

export interface FabricControlInput {
  message?: string;
  data?: unknown;
  triggerTurn?: boolean;
  binding?: FabricActorRunBinding;
}

export interface FabricControlRequestOptions {
  timeoutMs?: number;
  signal?: AbortSignal;
}

interface PendingControlRequest {
  resolve: (acceptance: FabricControlAcceptance) => void;
  reject: (error: Error) => void;
  /** Armed when the command's publish commits, with its wire deadline (smarty-dev#816). */
  timer?: NodeJS.Timeout;
  ownerHostId: string;
  ownerIdentityId: string;
  targetId: string;
  commandPublished: boolean;
  cancellationRequested?: boolean;
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
  #offset: number;
  #lastSequence: number;
  #timer: NodeJS.Timeout | undefined;
  #polling: Promise<void> | undefined;
  #closed = false;
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
    this.#seen = new MeshStore(
      path.join(mesh.root, "control-seen", createHash("sha256").update(options.hostId).digest("hex").slice(0, 32)),
      mesh.maxEventBytes,
      mesh.maxReadEvents,
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
    this.#timer = setInterval(() => void this.#poll().catch(() => undefined), this.#pollMs);
    this.#timer.unref();
  }

  async request(
    ownerHostId: string,
    targetId: string,
    operation: FabricControlOperation,
    input: FabricControlInput = {},
    ownerIdentityId = ownerHostId,
  ): Promise<FabricControlResult> {
    const { commandId, acceptance } = await this.#requestAcceptance(
      ownerHostId,
      targetId,
      operation,
      input,
      ownerIdentityId,
      { timeoutMs: this.#ackTimeoutMs },
    );
    return {
      queued: true,
      messageId: acceptance.messageId ?? commandId,
      routed: "mesh",
      acknowledged: true,
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
  ): Promise<{ commandId: string; acceptance: FabricControlAcceptance }> {
    if (!this.options.enabled) {
      throw new Error("Fabric mesh is disabled; cannot control a remote participant");
    }
    if (!ownerHostId.trim()) throw new Error("Remote participant has no execution owner");
    if (options.signal?.aborted) throw new Error(`Remote Fabric request cancelled: ${targetId}`);
    const timeoutMs = Math.max(
      this.#pollMs * 4,
      Math.min(MAX_CONTROL_TIMEOUT_MS, Math.floor(options.timeoutMs ?? this.#ackTimeoutMs)),
    );
    const commandId = randomUUID();
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
      };
      pendingRequest = pending;
      this.#pending.set(commandId, pending);
      if (options.signal) {
        const onAbort = (): void => {
          const cancelled = this.#clearPending(commandId);
          if (cancelled) void this.#publishCancellation(commandId, cancelled);
          reject(new Error(`Remote Fabric request cancelled: ${targetId}`));
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
      await this.mesh.publish({
        topic: CONTROL_TOPIC,
        kind: operation,
        from: this.identity,
        to: ownerHostId,
        // Stamped at commit (smarty-dev#816): the owner gets the whole timeout, not what is
        // left after this sender waited for the mesh lock (2-3 s under load).
        data: (committedAt: number): FabricControlCommand => ({
          version: 1,
          commandId,
          targetId,
          operation,
          replyTo: this.options.hostId,
          ...(input.message !== undefined ? { message: input.message } : {}),
          ...(input.data !== undefined ? { data: input.data } : {}),
          ...(input.triggerTurn !== undefined ? { triggerTurn: input.triggerTurn } : {}),
          ...(input.binding !== undefined ? { binding: input.binding } : {}),
          requestedAt: committedAt,
          deadlineAt: committedAt + timeoutMs,
        }),
      });
      pendingRequest!.commandPublished = true;
      // The wait starts at commit, like the owner's deadline: the lock wait before it (bounded by
      // the mesh lock timeout) is not taken from the timeout. A request cancelled or closed
      // meanwhile is no longer pending and is not revived.
      if (this.#pending.get(commandId) === pendingRequest!) {
        pendingRequest!.timer = setTimeout(() => {
          const timedOut = this.#clearPending(commandId);
          if (!timedOut) return;
          void this.#publishCancellation(commandId, timedOut);
          // The outcome is unknown: the owner may have admitted the command before its
          // deadline. A retry is a new command, so it can deliver the message twice.
          timedOut.reject(new Error(
            `Timed out waiting for the remote Fabric owner to acknowledge ${targetId}; ` +
              "the outcome is unknown and it may still be delivered, so a retry can deliver it twice.",
          ));
        }, timeoutMs + ackGraceMs);
        pendingRequest!.timer.unref();
      }
      if (pendingRequest!.cancellationRequested) {
        await this.#publishCancellation(commandId, pendingRequest!);
      }
      const acknowledged = await acceptance;
      if (!acknowledged.accepted) {
        throw new Error(acknowledged.error || "Remote Fabric owner rejected command for " + targetId);
      }
      return { commandId, acceptance: acknowledged };
    } catch (error) {
      const cancelled = this.#clearPending(commandId);
      if (cancelled) void this.#publishCancellation(commandId, cancelled);
      throw error;
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
    return pending;
  }

  async #publishCancellation(
    commandId: string,
    pending: PendingControlRequest,
  ): Promise<void> {
    if (!pending.commandPublished) {
      pending.cancellationRequested = true;
      return;
    }
    const requestedAt = Date.now();
    await this.mesh.publish({
      topic: CONTROL_TOPIC,
      kind: "cancel",
      from: this.identity,
      to: pending.ownerHostId,
      data: {
        version: 1,
        commandId: randomUUID(),
        targetId: pending.targetId,
        operation: "cancel",
        cancelCommandId: commandId,
        replyTo: this.options.hostId,
        requestedAt,
        deadlineAt: requestedAt + this.#ackTimeoutMs,
      } satisfies FabricControlCommand,
    }).catch(() => undefined);
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = undefined;
    await this.#polling?.catch(() => undefined);
    await this.#drain().catch(() => undefined);
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
    this.#handler = undefined;
  }

  async #poll(): Promise<void> {
    if (this.#closed || !this.options.enabled) return;
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
    while (true) {
      const tail = this.mesh.tail(this.#offset, 100);
      for (const event of tail.events) {
        if (event.sequence <= this.#lastSequence) continue;
        // An event is consumed only once handled: a command that hit a lock timeout throws,
        // and the next poll reads this page again from it (smarty-dev#424). Each retry is
        // bounded by the command's deadline plus the acknowledgement grace.
        if (event.to === this.options.hostId) {
          if (event.topic === ACK_TOPIC) this.#acceptAcknowledgement(event);
          else if (event.topic === CONTROL_TOPIC) await this.#acceptCommand(event);
        }
        this.#lastSequence = event.sequence;
      }
      this.#offset = tail.nextOffset;
      if (tail.events.length < 100) break;
    }
    await this.#cleanupSeen(Date.now()).catch(() => undefined);
  }

  #acceptAcknowledgement(event: MeshEvent): void {
    if (!isObject(event.data) || typeof event.data.commandId !== "string") return;
    const pending = this.#pending.get(event.data.commandId);
    if (
      !pending ||
      event.data.version !== 1 ||
      event.data.targetId !== pending.targetId ||
      event.from.id !== pending.ownerIdentityId
    ) {
      return;
    }
    this.#clearPending(event.data.commandId);
    pending.resolve({
      accepted: event.data.accepted === true,
      ...(typeof event.data.messageId === "string" ? { messageId: event.data.messageId } : {}),
      ...(Object.prototype.hasOwnProperty.call(event.data, "result")
        ? { result: event.data.result }
        : {}),
      ...(typeof event.data.error === "string" ? { error: event.data.error } : {}),
    });
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
      if (answerable) {
        await this.#publishAcknowledgement(command, {
          accepted: false,
          error: "Fabric control command expired",
        });
      }
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
        // may have passed since admission. An expired command is recorded, not run.
        if (Date.now() > deadlineAt) acceptance = { accepted: false, error: "Fabric control command expired" };
        else acceptance = this.#handler
          ? await this.#handler(command, from, controller.signal)
          : { accepted: false, error: "Fabric owner has no control handler" };
      } catch (error) {
        acceptance = {
          accepted: false,
          error: error instanceof Error ? error.message : String(error),
        };
      }
      acceptance = this.#boundedAcceptance(acceptance);
      try {
        await this.#seen.put({
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
      } catch (error) {
        // A conflict means another owner holds this claim; a lock timeout wrote nothing, and
        // the command has run, so its sender still gets the outcome.
        if (!isLockTimeout(error)) return;
      }
      try {
        await this.#publishAcknowledgement(command, acceptance);
      } catch (error) {
        // An "ask" runs detached from the drain, which has consumed it: nothing retries it, so
        // its outcome is not kept (its sender's result wait times out instead).
        if (command.operation !== "ask") {
          this.#unpublished.set(command.commandId, {
            acceptance,
            expiresAt: deadlineAt + MAX_CONTROL_ACK_GRACE_MS + this.#pollMs * 4,
          });
        }
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
      if (controlAcceptanceBytes(acceptance) <= this.mesh.maxEventBytes - 2_048) {
        return acceptance;
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
          ...(Object.prototype.hasOwnProperty.call(acceptance, "result")
            ? { result: acceptance.result }
            : {}),
          ...(acceptance.error ? { error: acceptance.error } : {}),
        },
      })
      .catch((error: unknown) => {
        // A lock timeout published nothing: the caller retries it (smarty-dev#424).
        if (isLockTimeout(error)) throw error;
      });
  }
}
