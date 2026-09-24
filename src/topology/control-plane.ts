import { createHash, randomUUID } from "node:crypto";
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
const IDEMPOTENCY_KEY = /^[A-Za-z0-9._:@/-]{1,200}$/;

/** Validates a caller-chosen message id used for at-most-once control delivery. */
export const assertFabricIdempotencyKey = (key: string): void => {
  if (!IDEMPOTENCY_KEY.test(key)) {
    throw new Error("messageId must be 1-200 characters of letters, digits and . _ : @ / -");
  }
};

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
  /** Caller-chosen id: repeating it after a timeout re-acknowledges instead of delivering twice. */
  idempotencyKey?: string;
}

interface PendingControlRequest {
  resolve: (acceptance: FabricControlAcceptance) => void;
  reject: (error: Error) => void;
  timer: NodeJS.Timeout;
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
  readonly #pollMs: number;
  readonly #ackTimeoutMs: number;
  #offset: number;
  #lastSequence: number;
  #timer: NodeJS.Timeout | undefined;
  #polling: Promise<void> | undefined;
  #closed = false;
  #handler: FabricControlHandler | undefined;
  #seenCleanupAt = 0;

  constructor(
    readonly mesh: MeshStore,
    readonly identity: MeshIdentity,
    readonly options: FabricControlPlaneOptions,
  ) {
    this.#pollMs = Math.max(20, options.pollMs ?? DEFAULT_POLL_MS);
    this.#ackTimeoutMs = Math.max(this.#pollMs * 4, options.acknowledgementTimeoutMs ?? DEFAULT_ACK_TIMEOUT_MS);
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
    options: Pick<FabricControlRequestOptions, "idempotencyKey"> = {},
  ): Promise<FabricControlResult> {
    const { commandId, acceptance } = await this.#requestAcceptance(
      ownerHostId,
      targetId,
      operation,
      input,
      ownerIdentityId,
      { timeoutMs: this.#ackTimeoutMs, ...options },
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
    if (options.idempotencyKey !== undefined) assertFabricIdempotencyKey(options.idempotencyKey);
    // A caller-chosen key gives a stable command id, namespaced by this sender: the
    // owner's existing dedupe then re-acknowledges a retry instead of delivering twice.
    const commandId = options.idempotencyKey === undefined
      ? randomUUID()
      : "idem:" + createHash("sha256").update(`${this.identity.id}\0${options.idempotencyKey}`).digest("hex").slice(0, 40);
    if (this.#pending.has(commandId)) {
      throw new Error(`A Fabric request with this messageId is already waiting for ${targetId}`);
    }
    const requestedAt = Date.now();
    const ackGraceMs = Math.min(MAX_CONTROL_ACK_GRACE_MS, 2 * timeoutMs);
    let pendingRequest: PendingControlRequest;
    const acceptance = new Promise<FabricControlAcceptance>((resolve, reject) => {
      const timer = setTimeout(() => {
        const timedOut = this.#clearPending(commandId);
        if (timedOut) void this.#publishCancellation(commandId, timedOut);
        reject(new Error(
          `Timed out waiting for the remote Fabric owner to acknowledge ${targetId}; ` +
            (options.idempotencyKey === undefined
              ? "it may still be delivered. Pass a messageId to retry without delivering twice."
              : `it may still be delivered. Retrying with messageId "${options.idempotencyKey}" delivers it at most once.`),
        ));
      }, timeoutMs + ackGraceMs);
      timer.unref();
      const pending: PendingControlRequest = {
        resolve,
        reject,
        timer,
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
        data: {
          version: 1,
          commandId,
          targetId,
          operation,
          replyTo: this.options.hostId,
          ...(input.message !== undefined ? { message: input.message } : {}),
          ...(input.data !== undefined ? { data: input.data } : {}),
          ...(input.triggerTurn !== undefined ? { triggerTurn: input.triggerTurn } : {}),
          ...(input.binding !== undefined ? { binding: input.binding } : {}),
          requestedAt,
          deadlineAt: requestedAt + timeoutMs,
        } satisfies FabricControlCommand,
      });
      pendingRequest!.commandPublished = true;
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
    while (true) {
      const tail = this.mesh.tail(this.#offset, 100);
      this.#offset = tail.nextOffset;
      for (const event of tail.events) {
        if (event.sequence <= this.#lastSequence) continue;
        this.#lastSequence = event.sequence;
        if (event.to !== this.options.hostId) continue;
        if (event.topic === ACK_TOPIC) this.#acceptAcknowledgement(event);
        else if (event.topic === CONTROL_TOPIC) await this.#acceptCommand(event);
      }
      if (tail.events.length < 100) break;
    }
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
    await this.#cleanupSeen(now);
    const deadlineAt = Math.min(
      command.deadlineAt ?? command.requestedAt + this.#ackTimeoutMs,
      command.requestedAt + MAX_CONTROL_TIMEOUT_MS,
    );
    if (now > deadlineAt || command.requestedAt - now > this.#ackTimeoutMs) {
      await this.#publishAcknowledgement(command, {
        accepted: false,
        error: "Fabric control command expired",
      });
      return;
    }

    const key = controlSeenKey(this.options.hostId, command.commandId);
    const duplicate = controlSeenRecord(this.mesh.get(key)?.value);
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
            error: this.#activeCommands.has(command.commandId)
              ? "Fabric control command is still running; retry later with the same messageId"
              : "Fabric control outcome is indeterminate after owner restart",
          },
        );
      }
      return;
    }

    let claim;
    try {
      claim = await this.mesh.put({
        key,
        value: {
          format: 1,
          hostId: this.options.hostId,
          commandId: command.commandId,
          targetId: command.targetId,
          expiresAt: deadlineAt + this.#ackTimeoutMs,
        } satisfies FabricControlSeenRecord,
        identity: this.identity,
        ifVersion: 0,
      });
    } catch {
      const raced = controlSeenRecord(this.mesh.get(key)?.value);
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

    const execution = this.#executeClaimedCommand(
      command,
      event.from,
      key,
      claim.version,
      deadlineAt,
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
        acceptance = this.#handler
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
        await this.mesh.put({
          key,
          value: {
            format: 1,
            hostId: this.options.hostId,
            commandId: command.commandId,
            targetId: command.targetId,
            expiresAt: Math.max(deadlineAt, Date.now()) + this.#ackTimeoutMs,
            acceptance,
          } satisfies FabricControlSeenRecord,
          identity: this.identity,
          ifVersion: claimVersion,
        });
      } catch {
        return;
      }
      await this.#publishAcknowledgement(command, acceptance);
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

  async #cleanupSeen(now: number): Promise<void> {
    if (now - this.#seenCleanupAt < this.#ackTimeoutMs) return;
    this.#seenCleanupAt = now;
    const candidates = this.mesh.listAll(CONTROL_SEEN_PREFIX).flatMap((entry) => {
      const record = controlSeenRecord(entry.value);
      return !record || record.expiresAt < now ? [{ entry, record }] : [];
    });
    if (candidates.length === 0) return;

    const sought = new Set(
      candidates.flatMap(({ record }) => record ? [record.commandId] : []),
    );
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

    await Promise.allSettled(
      candidates
        .filter(({ record }) => !record || !retained.has(record.commandId))
        .map(({ entry }) => this.mesh.delete({ key: entry.key, ifVersion: entry.version })),
    );
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
      .catch(() => undefined);
  }
}
