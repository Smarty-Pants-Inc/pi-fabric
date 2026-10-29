import { Readable } from "node:stream";
import {
  DeliverPolicy,
  JetStreamApiError,
  JetStreamError,
  StorageType,
  type ConsumerConfig,
  type ConsumerMessages,
  type JetStreamClient,
  type JsMsg,
  type OrderedPushConsumerOptions,
  type PushConsumer,
} from "@nats-io/jetstream";
import { Kvm, type KV, type KvEntry } from "@nats-io/kv";
import { NoRespondersError, PermissionViolationError, RequestError, TimeoutError } from "@nats-io/transport-node";
import { NATS_KV_FLOW_CONTROL_WARNING, type SubscribeNatsStatus } from "./connection.js";
import type { TransportKv, TransportKvEntry, TransportKvEvent } from "../types.js";
import { KvConflictError, TransportDeniedError, TransportInvalidError, TransportUnavailableError, TransportUncertainError } from "../types.js";
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
const WATCH_STORAGE_FALLBACK_WARNED = new Set<string>();
const KV_OPERATION_TIMEOUT_MS = 5_000;
const KV_WATCH_CLEANUP_TIMEOUT_MS = 1_000;
const kvOperationSignal = (signal?: AbortSignal): AbortSignal => signal
  ? AbortSignal.any([signal, AbortSignal.timeout(KV_OPERATION_TIMEOUT_MS)])
  : AbortSignal.timeout(KV_OPERATION_TIMEOUT_MS);
const abortable = <T>(operation: () => Promise<T>, signal: AbortSignal): Promise<T> => new Promise((resolve, reject) => {
  const cleanup = (): void => signal.removeEventListener("abort", onAbort);
  const onAbort = (): void => { cleanup(); reject(signal.reason); };
  signal.addEventListener("abort", onAbort, { once: true });
  if (signal.aborted) { onAbort(); return; }
  let pending: Promise<T>;
  try { pending = operation(); }
  catch (error) { cleanup(); reject(error); return; }
  void pending.then(value => { cleanup(); resolve(value); }, error => { cleanup(); reject(error); });
});

export interface NatsKvPolicy {
  readonly bucket: string;
  readonly ttlMs?: number;
  readonly markerTtlMs?: number;
  readonly replicas?: number;
}

const apiCode = (error: unknown): number | undefined => error instanceof JetStreamApiError ? error.apiError().err_code : undefined;
const isUnavailable = (error: unknown): boolean => typeof error === "object" && error !== null &&
  (error as { readonly code?: unknown }).code === "FABRIC_TRANSPORT_UNAVAILABLE";
const abort = (signal?: AbortSignal): void => signal?.throwIfAborted();

// Only recognized broker/request failures are transient. In particular, do not
// unwrap arbitrary causes or retry auth, invalid consumer config, or bad KV data.
const WATCH_NOT_READY = /^(?:no (?:jetstream )?meta leader|(?:jetstream )?cluster (?:is )?not ready|(?:stream|consumer) leader (?:is )?not (?:yet )?elected|consumer (?:is )?not (?:yet )?current)$/i;
export const kvWatchRetryDelayMs = (attempt: number, jitter: number): number => {
  const attemptIndex = Number.isSafeInteger(attempt) && attempt > 0 ? attempt - 1 : 0;
  const base = Math.min(5_000, 250 * 2 ** Math.min(attemptIndex, 5));
  const sample = Number.isFinite(jitter) ? Math.max(0, Math.min(1, jitter)) : 0.5;
  return Math.round(Math.min(5_000, Math.max(250, base * (0.8 + sample * 0.4))));
};

export const kvWatchRetryCode = (error: unknown): string | undefined => {
  for (let depth = 0; depth < 6 && error instanceof Error; depth++) {
    if (error instanceof JetStreamApiError) {
      const api = error.apiError();
      if ([401, 403].includes(api.code) || [10039, 10076].includes(api.err_code)) return undefined;
      if (api.code === 503 || api.code === 408 || [10008, 10009].includes(api.err_code) || WATCH_NOT_READY.test(api.description)) {
        return Number.isSafeInteger(api.err_code) ? `JS_API_${api.err_code}` : `JS_STATUS_${api.code}`;
      }
      return undefined;
    }
    if (error instanceof TimeoutError) return "TIMEOUT";
    if (error instanceof NoRespondersError) return "NO_RESPONDERS";
    if (error instanceof JetStreamError && error.name === "JetStreamStatusError") {
      const code = (error as JetStreamError & { readonly code?: number }).code;
      return code === 503 || code === 408 ? `JS_STATUS_${code}` : undefined;
    }
    // The SDK wraps request no-responders in JetStreamNotEnabled, which is not
    // publicly exported. The name alone is not enough: require a recognized cause.
    if (!(error instanceof RequestError) && error.name !== "JetStreamNotEnabled") return undefined;
    error = error.cause;
  }
  return undefined;
};

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
    status.storage !== StorageType.File || status.replicas !== policy.replicas || status.maxValueSize !== KV_MAX_VALUE_SIZE ||
    status.streamInfo.config.allow_direct !== false) {
    throw new TransportInvalidError(`NATS KV configuration mismatch: ${policy.bucket}`);
  }
};

export const openNatsKv = async (js: JetStreamClient, policy: NatsKvPolicy, provision = true, onStatus?: SubscribeNatsStatus): Promise<NatsKvStore> => {
  const resolved: Required<NatsKvPolicy> = {
    bucket: policy.bucket,
    ttlMs: policy.ttlMs ?? (policy.bucket === PRESENCE_BUCKET ? PRESENCE_TTL_MS : LEASE_TTL_MS),
    markerTtlMs: policy.markerTtlMs ?? KV_MARKER_TTL_MS,
    replicas: policy.replicas === undefined ? 1 : policy.replicas,
  };
  if (![resolved.ttlMs, resolved.markerTtlMs].every((value) => Number.isSafeInteger(value) && value > 0)) {
    throw new TransportInvalidError("KV TTL values must be positive integers");
  }
  if (!Number.isSafeInteger(resolved.replicas) || resolved.replicas <= 0) {
    throw new TransportInvalidError("KV replicas must be a positive safe integer");
  }
  const manager = new Kvm(js);
  const options = {
    ttl: resolved.ttlMs,
    markerTTL: resolved.markerTtlMs,
    history: 1,
    maxValueSize: KV_MAX_VALUE_SIZE,
    storage: StorageType.File,
    replicas: resolved.replicas,
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

  constructor(bucket: KV, policy: Required<Omit<NatsKvPolicy, "replicas">> & Pick<NatsKvPolicy, "replicas">, js: JetStreamClient, onStatus?: SubscribeNatsStatus) {
    this.#bucket = bucket;
    this.#policy = { ...policy, replicas: policy.replicas === undefined ? 1 : policy.replicas };
    this.#js = js;
    this.#onStatus = onStatus;
    this.ttlMs = policy.ttlMs;
  }

  async get<V>(key: string, signal?: AbortSignal): Promise<TransportKvEntry<V> | undefined> {
    abort(signal);
    splitKey(key);
    const requestSignal = kvOperationSignal(signal);
    const entry = await abortable(() => this.#bucket.get(key), requestSignal);
    abort(requestSignal);
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
    const requestSignal = kvOperationSignal(signal);
    try {
      // The official KV delete API has no AbortSignal parameter; stop awaiting it
      // when the caller's shutdown/operation budget expires. The remote CAS may win.
      await abortable(() => this.#bucket.delete(key, { previousSeq: ifRevision }), requestSignal);
    } catch (error) {
      if (requestSignal.aborted) throw new TransportUncertainError(key, undefined, { cause: requestSignal.reason });
      if (CONFLICT_CODES.has(apiCode(error) ?? -1)) return false;
      throw publishFailure(error, key, undefined);
    }
    if (requestSignal.aborted) throw new TransportUncertainError(key, undefined, { cause: requestSignal.reason });
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
    let wakeRetry: (() => void) | undefined;
    let retryAttempt = 0;
    let retryDelay = 0;
    let retryCode = "";
    const diagnostic = (outcome: string, code = retryCode): void => {
      console.warn(`[fabric:nats:kv-watch] ${JSON.stringify({ outcome, code, bucket: this.#policy.bucket, attempt: retryAttempt })}`);
    };
    const stop = (): void => { releaseReconnect?.(); wakeRetry?.(); endEpoch?.(); };
    const pauseUntilReconnect = (): void => {
      if (!disconnected) {
        disconnected = true;
        reconnect = new Promise<void>((resolve) => { releaseReconnect = resolve; });
      }
      invalidate?.();
    };
    // Keep connection observation across snapshot generations, including setup and
    // the reconnect wait. Re-subscribing per generation can miss a disconnect.
    signal.addEventListener("abort", stop, { once: true });
    const unsubscribe = this.#onStatus?.((status) => {
      if (status.type === "disconnect") pauseUntilReconnect();
      if (status.type === "reconnect" || status.type === "close") {
        disconnected = false;
        closed = status.type === "close";
        if (closed) { invalidate?.(); wakeRetry?.(); }
        releaseReconnect?.();
      }
      if (!flowControlFailure && status.type === "error" && status.error instanceof PermissionViolationError && status.error.operation === "publish" &&
        status.error.subject.startsWith(flowControlPrefix)) {
        flowControlFailure = new TransportDeniedError(NATS_KV_FLOW_CONTROL_WARNING, { cause: status.error });
        wakeRetry?.();
        endEpoch?.();
      }
    });
    try {
      while (!signal.aborted && !closed) {
        // Core reconnect is not a JetStream-readiness signal. Retry API failures
        // independently, but keep the verified-connection gate and cancellation.
        if (retryDelay > 0) {
          await new Promise<void>((resolve) => {
            const timer = setTimeout(resolve, retryDelay);
            wakeRetry = () => { clearTimeout(timer); resolve(); };
            if (signal.aborted || closed || flowControlFailure) wakeRetry();
          });
          wakeRetry = undefined;
          retryDelay = 0;
        }
        if (flowControlFailure) throw flowControlFailure;
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
            // Ordered watchers replay from the file-backed stream; persisting their
            // disposable cursors only adds synced consumer-state writes. SDK 3.4
            // forwards ConsumerConfig fields omitted by OrderedPushConsumerOptions.
            const options: Partial<OrderedPushConsumerOptions> & Pick<ConsumerConfig, "mem_storage"> = {
              filter_subjects: `${subjectPrefix}${prefix}`,
              deliver_policy: DeliverPolicy.LastPerSubject,
              mem_storage: true,
            };
            let consumer: PushConsumer;
            try {
              consumer = await this.#js.consumers.getPushConsumer(stream, options);
            } catch (error) {
              // Compatibility only: memory/account quota refusals, never auth,
              // invalid config, consumer-count limits or disk-specific failures.
              if (!(error instanceof JetStreamApiError) || [401, 403].includes(error.apiError().code) ||
                ![10028, 10002].includes(error.apiError().err_code)) throw error;
              if (signal.aborted || closed) return;
              if (!WATCH_STORAGE_FALLBACK_WARNED.has(this.#policy.bucket)) {
                WATCH_STORAGE_FALLBACK_WARNED.add(this.#policy.bucket);
                console.warn(`[fabric:nats:kv-watch] ${JSON.stringify({ outcome: "file-storage-fallback",
                  code: `JS_API_${error.apiError().err_code}`, bucket: this.#policy.bucket })}`);
              }
              const fileOptions = { ...options, mem_storage: false };
              consumer = await this.#js.consumers.getPushConsumer(stream, fileOptions);
            }
            const initial = await consumer.info(true);
            const apply = (msg: JsMsg): void => {
              if (reset || signal.aborted || ended) return;
              if (this.#onStatus?.isVerified?.() === false) { pauseUntilReconnect(); return; }
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
            // Gate failures can race the asynchronous `disconnect` status. Preserve the
            // directory gap and await verified reconnect rather than killing its watch task.
            if (!reset && !signal.aborted && !closed) {
              if (error instanceof TransportUnavailableError || isUnavailable(error)) {
                const verified = this.#onStatus?.isVerified?.();
                if (verified === false) pauseUntilReconnect();
                else if (this.#onStatus) gap();
                else throw error;
              } else throw error;
            }
          }
          for await (const event of queue) {
            if ((event as TransportKvEvent<V>).type === "ready" && retryAttempt > 0) {
              diagnostic("ready");
              retryAttempt = 0;
            }
            yield event as TransportKvEvent<V>;
          }
          if (flowControlFailure) failure = flowControlFailure;
          if (failure) throw failure;
          if (!reset) return;
        } catch (error) {
          if (signal.aborted || closed) return;
          const code = kvWatchRetryCode(error);
          if (!code || flowControlFailure) throw error;
          // Discard any partial snapshot before starting a fresh ordered epoch.
          // A failed setup may not have entered the queue drain at all.
          if (!reset) { reset = true; end(); yield { type: "reset" }; }
          retryCode = code;
          retryAttempt++;
          retryDelay = kvWatchRetryDelayMs(retryAttempt, Math.random());
          diagnostic("retry");
        } finally {
          invalidate = undefined;
          endEpoch = undefined;
          messages?.stop();
          let cleanupTimer: ReturnType<typeof setTimeout> | undefined;
          try {
            // A disconnected core connection can leave the SDK iterator's close/status
            // promises pending. Stop locally, but never pin projection/transport teardown
            // on remote consumer cleanup.
            await Promise.race([
              (async () => {
                await messages?.close();
                await Promise.all([statusTask, closedTask]);
              })(),
              new Promise<void>(resolve => { cleanupTimer = setTimeout(resolve, KV_WATCH_CLEANUP_TIMEOUT_MS); }),
            ]);
          } finally {
            if (cleanupTimer !== undefined) clearTimeout(cleanupTimer);
            queue.destroy();
          }
        }
      }
    } catch (error) {
      if (retryAttempt > 0) { diagnostic("failed", "TERMINAL"); retryAttempt = 0; }
      throw error;
    } finally {
      if (retryAttempt > 0) diagnostic(signal.aborted ? "cancelled" : "closed");
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

export const openNatsKvForMachine = async (js: JetStreamClient, bucket: "leases" | "presence", machine: string, provision = true, onStatus?: SubscribeNatsStatus, replicas = 1): Promise<NatsKvStore> => {
  validateMachine(machine);
  return (await openNatsKv(js, { bucket: bucket === "leases" ? LEASES_BUCKET : PRESENCE_BUCKET, replicas }, provision, onStatus)).withMachine(machine);
};
