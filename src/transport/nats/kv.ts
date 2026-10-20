import { Readable } from "node:stream";
import {
  DeliverPolicy,
  JetStreamApiError,
  StorageType,
  type ConsumerMessages,
  type JetStreamClient,
  type JsMsg,
} from "@nats-io/jetstream";
import { Kvm, type KV, type KvEntry } from "@nats-io/kv";
import { PermissionViolationError } from "@nats-io/transport-node";
import { NATS_KV_FLOW_CONTROL_WARNING, type SubscribeNatsStatus } from "./connection.js";
import type { TransportKv, TransportKvEntry, TransportKvEvent } from "../types.js";
import { KvConflictError, TransportDeniedError, TransportInvalidError, TransportUncertainError } from "../types.js";
import { publishFailure } from "./errors.js";

export const LEASES_BUCKET = "fabric_leases";
export const PRESENCE_BUCKET = "fabric_presence";
export const PLACEMENT_TERMINAL_BUCKET = "fabric_placement_terminal";
export const PLACEMENT_TERMINAL_TTL_MS = 900_000;
export const LEASE_TTL_MS = 15_000;
export const PRESENCE_TTL_MS = 10 * 60_000;
export const KV_TTL_MS = LEASE_TTL_MS;
export const KV_MARKER_TTL_MS = 5 * 60_000;
export const KV_MAX_VALUE_SIZE = 64 * 1024;
export { KvConflictError as NatsKvConflictError } from "../types.js";
export type { TransportKvEntry as NatsKvEntry, TransportKvEvent as NatsKvEvent } from "../types.js";

const MACHINE = /^[a-z0-9][a-z0-9-]{0,31}$/;
const KEY_TOKEN = /^[A-Za-z0-9_-]{1,128}$/;
const CONFLICT_CODES = new Set([10071, 10164]);

export interface NatsKvPolicy {
  readonly bucket: string;
  readonly ttlMs?: number;
  readonly markerTtlMs?: number;
}

const apiCode = (error: unknown): number | undefined => error instanceof JetStreamApiError ? error.apiError().err_code : undefined;
const abort = (signal?: AbortSignal): void => signal?.throwIfAborted();

const validateMachine = (machine: string): string => {
  if (!MACHINE.test(machine)) throw new TransportInvalidError("invalid machine token");
  return machine;
};

const splitKey = (key: string): readonly [string, string, string] => {
  const parts = key.split(".");
  if (parts.length !== 3 || !MACHINE.test(parts[0]!) || !/^(h|p)$/.test(parts[1]!) || !KEY_TOKEN.test(parts[2]!)) {
    throw new TransportInvalidError("invalid KV key");
  }
  return parts as [string, string, string];
};

const validatePrefix = (prefix: string): void => {
  const parts = prefix.split(".");
  if (parts.length > 3 || parts.some((token, index) =>
    token === ">" ? index !== parts.length - 1 : token !== "*" && !KEY_TOKEN.test(token))) {
    throw new TransportInvalidError("invalid KV watch prefix");
  }
};

const decodeValue = <V>(key: string, value: Uint8Array, revision: number): TransportKvEntry<V> => {
  const [machine] = splitKey(key);
  let parsed: { owner?: unknown; value?: V };
  try { parsed = JSON.parse(new TextDecoder().decode(value)) as typeof parsed; }
  catch (cause) { throw new TransportInvalidError("invalid KV value", { cause }); }
  if (!parsed || typeof parsed.owner !== "string" || parsed.owner.length === 0 || !("value" in parsed)) throw new TransportInvalidError("invalid KV value");
  return { key, machine, owner: parsed.owner, value: parsed.value as V, revision };
};
const decode = <V>(entry: KvEntry): TransportKvEntry<V> => decodeValue(entry.key, entry.value, entry.revision);

const assertPolicy = async (bucket: KV, policy: Required<NatsKvPolicy>): Promise<void> => {
  const status = await bucket.status();
  if (status.ttl !== policy.ttlMs || status.history !== 1 || status.markerTTL !== policy.markerTtlMs ||
    status.storage !== StorageType.File || status.replicas !== 1 || status.maxValueSize !== KV_MAX_VALUE_SIZE ||
    status.streamInfo.config.allow_direct !== false) {
    throw new Error(`NATS KV configuration mismatch: ${policy.bucket}`);
  }
};

export const openNatsKv = async (js: JetStreamClient, policy: NatsKvPolicy, provision = true, onStatus?: SubscribeNatsStatus): Promise<NatsKvStore> => {
  const resolved: Required<NatsKvPolicy> = {
    bucket: policy.bucket,
    ttlMs: policy.ttlMs ?? (policy.bucket === PRESENCE_BUCKET ? PRESENCE_TTL_MS : LEASE_TTL_MS),
    markerTtlMs: policy.markerTtlMs ?? KV_MARKER_TTL_MS,
  };
  if (![resolved.ttlMs, resolved.markerTtlMs].every((value) => Number.isSafeInteger(value) && value > 0)) {
    throw new TransportInvalidError("KV TTL values must be positive integers");
  }
  const manager = new Kvm(js);
  const options = {
    ttl: resolved.ttlMs,
    markerTTL: resolved.markerTtlMs,
    history: 1,
    maxValueSize: KV_MAX_VALUE_SIZE,
    storage: StorageType.File,
    replicas: 1,
    allow_direct: false,
  };
  const bucket = provision ? await manager.create(resolved.bucket, options) : await manager.open(resolved.bucket, options);
  await assertPolicy(bucket, resolved);
  return new NatsKvStore(bucket, resolved, js, onStatus);
};

export class NatsKvStore implements TransportKv {
  readonly ttlMs: number;
  readonly #bucket: KV;
  readonly #policy: Required<NatsKvPolicy>;
  readonly #js: JetStreamClient;
  readonly #onStatus: SubscribeNatsStatus | undefined;
  #machine = "";

  constructor(bucket: KV, policy: Required<NatsKvPolicy>, js: JetStreamClient, onStatus?: SubscribeNatsStatus) {
    this.#bucket = bucket;
    this.#policy = policy;
    this.#js = js;
    this.#onStatus = onStatus;
    this.ttlMs = policy.ttlMs;
  }

  async get<V>(key: string, signal?: AbortSignal): Promise<TransportKvEntry<V> | undefined> {
    abort(signal);
    splitKey(key);
    const entry = await this.#bucket.get(key);
    abort(signal);
    return entry?.operation === "PUT" ? decode<V>(entry) : undefined;
  }

  async put<V>(key: string, owner: string, value: V, ifRevision: number, signal?: AbortSignal): Promise<number> {
    abort(signal);
    this.#assertWriter(key, ifRevision, true);
    if (typeof owner !== "string" || owner.length === 0) throw new TransportInvalidError("KV owner must be non-empty");
    let encoded: string;
    try {
      encoded = JSON.stringify({ owner, value });
      if (!("value" in JSON.parse(encoded))) throw new Error("value is not JSON serializable");
    } catch (cause) { throw new TransportInvalidError("invalid KV value", { cause }); }
    if (new TextEncoder().encode(encoded).byteLength > KV_MAX_VALUE_SIZE) throw new TransportInvalidError("KV value is too large");
    let revision: number;
    try {
      revision = ifRevision === 0
        ? await this.#bucket.create(key, encoded)
        : await this.#bucket.update(key, encoded, ifRevision);
    } catch (error) {
      if (CONFLICT_CODES.has(apiCode(error) ?? -1)) throw new KvConflictError(key, ifRevision);
      throw publishFailure(error, key, { owner, value });
    }
    if (signal?.aborted) throw new TransportUncertainError(key, { owner, value }, { cause: signal.reason });
    return revision;
  }

  async delete(key: string, ifRevision: number, signal?: AbortSignal): Promise<boolean> {
    abort(signal);
    this.#assertWriter(key, ifRevision, false);
    try {
      await this.#bucket.delete(key, { previousSeq: ifRevision });
    } catch (error) {
      if (CONFLICT_CODES.has(apiCode(error) ?? -1)) return false;
      throw publishFailure(error, key, undefined);
    }
    if (signal?.aborted) throw new TransportUncertainError(key, undefined, { cause: signal.reason });
    return true;
  }

  /** Raw runId-keyed Placement receipt. This is separate from machine-scoped Fabric KV. */
  async writePlacementTerminal(record: import("../../placement/terminal.js").PlacementTerminalRecord): Promise<void> {
    const terminalModule: typeof import("../../placement/terminal.js") = await import("../../placement/terminal.js");
    const { PlacementTerminalWriteError } = terminalModule;
    try {
      if (this.#policy.bucket !== PLACEMENT_TERMINAL_BUCKET) throw new PlacementTerminalWriteError();
      terminalModule.assertPlacementTerminalRecord(record);
      const key = record.placementRunId;
      const encoded = JSON.stringify(record);
      if (new TextEncoder().encode(encoded).byteLength > KV_MAX_VALUE_SIZE) throw new PlacementTerminalWriteError();
      const same = (entry: KvEntry | null | undefined): boolean => {
        if (!entry || entry.operation !== "PUT") return false;
        try { return new TextDecoder("utf-8", { fatal: true }).decode(entry.value) === encoded; }
        catch { return false; }
      };
      const existing = await this.#bucket.get(key);
      if (existing) {
        if (same(existing)) return;
        throw new PlacementTerminalWriteError();
      }
      try { await this.#bucket.create(key, encoded); }
      catch (error) {
        if (same(await this.#bucket.get(key).catch(() => undefined))) return;
        throw new PlacementTerminalWriteError(error);
      }
      if (!same(await this.#bucket.get(key))) throw new PlacementTerminalWriteError();
    } catch (error) {
      if (error instanceof PlacementTerminalWriteError) throw error;
      throw new PlacementTerminalWriteError(error);
    }
  }

  #assertWriter(key: string, revision: number, create: boolean): void {
    const [machine] = splitKey(key);
    if (machine !== this.#machine) throw new TransportDeniedError("KV write is outside the credential's machine prefix");
    if (!Number.isSafeInteger(revision) || revision < (create ? 0 : 1)) throw new TransportInvalidError("invalid KV revision");
  }

  async *watch<V>(prefix: string, signal: AbortSignal): AsyncIterable<TransportKvEvent<V>> {
    validatePrefix(prefix);
    if (signal.aborted) return;
    const stream = `KV_${this.#policy.bucket}`;
    const subjectPrefix = `$KV.${this.#policy.bucket}.`;
    const flowControlPrefix = `$JS.FC.${stream}.`;
    let disconnected = false;
    let closed = false;
    let reconnect = Promise.resolve();
    let releaseReconnect: (() => void) | undefined;
    let invalidate: (() => void) | undefined;
    let endEpoch: (() => void) | undefined;
    let flowControlFailure: Error | undefined;
    const stop = (): void => { releaseReconnect?.(); endEpoch?.(); };
    // Keep connection observation across snapshot generations, including setup and
    // the reconnect wait. Re-subscribing per generation can miss a disconnect.
    signal.addEventListener("abort", stop, { once: true });
    const unsubscribe = this.#onStatus?.((status) => {
      if (status.type === "disconnect") {
        if (!disconnected) {
          disconnected = true;
          reconnect = new Promise<void>((resolve) => { releaseReconnect = resolve; });
        }
        invalidate?.();
      }
      if (status.type === "reconnect" || status.type === "close") {
        disconnected = false;
        closed = status.type === "close";
        if (closed) invalidate?.();
        releaseReconnect?.();
      }
      if (!flowControlFailure && status.type === "error" && status.error instanceof PermissionViolationError && status.error.operation === "publish" &&
        status.error.subject.startsWith(flowControlPrefix)) {
        flowControlFailure = new TransportDeniedError(NATS_KV_FLOW_CONTROL_WARNING, { cause: status.error });
        endEpoch?.();
      }
    });
    try {
      while (!signal.aborted && !closed) {
        if (disconnected) await reconnect;
        if (signal.aborted || closed) return;
        const queue = new Readable({ objectMode: true, read() {} });
        let messages: ConsumerMessages | undefined;
        let statusTask: Promise<void> | undefined;
        let closedTask: Promise<void> | undefined;
        let reset = false;
        let ready = false;
        let ended = false;
        let failure: unknown = flowControlFailure;
        let delivered = 0;
        const emit = (event: TransportKvEvent<V>): void => { if (!ended && !queue.destroyed) queue.push(event); };
        const end = (): void => {
          messages?.stop();
          if (!ended && !queue.destroyed) queue.push(null);
          ended = true;
        };
        const gap = (): void => {
          if (reset || signal.aborted || ended) return;
          reset = true;
          emit({ type: "reset" });
          end();
        };
        const markReady = (): void => {
          if (ready || reset || signal.aborted || closed || ended) return;
          ready = true;
          emit({ type: "ready" });
        };
        invalidate = gap;
        endEpoch = end;
        try {
          try {
            // Official ordered push API: creation metadata covers an empty snapshot;
            // pending=0 or an in-order heartbeat covers the last snapshot delivery.
            // Do not count the initial pending entries: deletes/expiry can remove them.
            const consumer = await this.#js.consumers.getPushConsumer(stream, {
              filter_subjects: `${subjectPrefix}${prefix}`,
              deliver_policy: DeliverPolicy.LastPerSubject,
            });
            const initial = await consumer.info(true);
            const apply = (msg: JsMsg): void => {
              if (reset || signal.aborted || ended) return;
              try {
                const info = msg.info;
                // Recreation status is asynchronous; reject a new generation's data
                // synchronously, before it can leak into the old ready view.
                if (info.consumer !== initial.name || info.deliverySequence !== delivered + 1) {
                  gap();
                  return;
                }
                delivered = info.deliverySequence;
                const key = msg.subject.slice(subjectPrefix.length);
                const operation = msg.headers?.get("KV-Operation");
                // Server-generated expiry markers have no KV-Operation header.
                const expired = msg.headers?.get("Nats-Marker-Reason") === "MaxAge";
                if (expired || operation === "DEL" || operation === "PURGE") {
                  const [machine] = splitKey(key);
                  emit({ type: "gone", key, machine, revision: info.streamSequence,
                    reason: expired ? "expired" : "deleted" });
                } else {
                  emit({ type: "put", entry: decodeValue<V>(key, msg.data, info.streamSequence) });
                }
                if (info.pending === 0) markReady();
              } catch (error) {
                failure = error;
                end();
              }
            };
            if (initial.num_pending === 0) markReady();
            if (!reset && !signal.aborted && !closed) {
              messages = await consumer.consume({ callback: apply });
              if (failure || flowControlFailure) messages.stop();
              statusTask = (async () => {
                for await (const status of messages!.status()) {
                  switch (status.type) {
                    case "heartbeats_missed":
                    case "ordered_consumer_recreated":
                    case "consumer_deleted":
                    case "consumer_not_found":
                    case "stream_not_found":
                    case "reset":
                      gap();
                      break;
                    case "heartbeat":
                      if (status.lastConsumerSequence > delivered) gap();
                      else if (status.lastConsumerSequence === delivered) markReady();
                      break;
                  }
                }
              })();
              closedTask = messages.closed().then((error) => {
                if (error) { failure = error; end(); }
                else if (!signal.aborted) gap();
                else end();
              });
              if (reset || signal.aborted || closed) end();
            }
          } catch (error) {
            // A disconnect during consumer creation still owes the caller reset,
            // then a fresh snapshot after reconnect, not a terminal request error.
            if (!reset && !signal.aborted && !closed) throw error;
          }
          for await (const event of queue) yield event as TransportKvEvent<V>;
          if (flowControlFailure) failure = flowControlFailure;
          if (failure) throw failure;
          if (!reset) return;
        } finally {
          invalidate = undefined;
          endEpoch = undefined;
          await messages?.close();
          await Promise.all([statusTask, closedTask]);
          queue.destroy();
        }
      }
    } finally {
      signal.removeEventListener("abort", stop);
      unsubscribe?.();
    }
  }

  /** Bind writes to one machine; reads and watches remain fleet-wide. */
  withMachine(machine: string): NatsKvStore {
    const copy = new NatsKvStore(this.#bucket, this.#policy, this.#js, this.#onStatus);
    copy.#machine = validateMachine(machine);
    return copy;
  }
}

export const openNatsKvForMachine = async (js: JetStreamClient, bucket: "leases" | "presence", machine: string, provision = true, onStatus?: SubscribeNatsStatus): Promise<NatsKvStore> => {
  validateMachine(machine);
  return (await openNatsKv(js, { bucket: bucket === "leases" ? LEASES_BUCKET : PRESENCE_BUCKET }, provision, onStatus)).withMachine(machine);
};
