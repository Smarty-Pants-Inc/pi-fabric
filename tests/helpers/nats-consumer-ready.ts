import { AsyncLocalStorage } from "node:async_hooks";
import { connect } from "@nats-io/transport-node";
import { JetStreamApiError, jetstream, jetstreamManager, type Consumer, type Consumers } from "@nats-io/jetstream";

type ReadinessOptions = { timeoutMs?: number };
type Capture = { stream: string; receivers: Consumer[]; active: boolean };
const captures = new AsyncLocalStorage<Capture>();
let captureUsers = 0;
let restoreGet: (() => void) | undefined;

/** Observe the actual SDK get selected by the product, not LIST or a late ACK from an abandoned get.
 * Record only when that returned object's original consume is invoked. No extra request/await is inserted
 * between get and the product's first NEXT; cached info is read after the original operation returns.
 * Async-local scopes keep simultaneous opens (even of the same stream) from borrowing each other's receiver. */
export const captureNatsConsumer = async <T,>(server: string, stream: string, operation: () => Promise<T>): Promise<{ value: T; name: string }> => {
  const nc = await connect({ servers: [server], ignoreClusterUpdates: true, timeout: 1_000 });
  try {
    if (captureUsers === 0) {
      const prototype = Object.getPrototypeOf(jetstream(nc).consumers) as Consumers;
      const get = prototype.get;
      prototype.get = async function (this: Consumers, ...args: Parameters<Consumers["get"]>) {
        const capture = captures.getStore();
        const receiver = await get.apply(this, args);
        if (capture?.active && capture.stream === args[0]) {
          const consume = receiver.consume;
          receiver.consume = function (...options: Parameters<Consumer["consume"]>) {
            receiver.consume = consume;
            if (capture.active) capture.receivers.push(receiver);
            return consume.apply(this, options);
          };
        }
        return receiver;
      };
      restoreGet = () => { prototype.get = get; };
    }
    captureUsers++;
    const capture: Capture = { stream, receivers: [], active: true };
    try {
      const value = await captures.run(capture, operation);
      if (capture.receivers.length !== 1) throw new Error(`expected one actual SDK receiver for ${stream}, observed ${capture.receivers.length}`);
      const info = await capture.receivers[0]!.info(true);
      if (info.stream_name !== stream || !info.name) throw new Error(`actual SDK receiver identity mismatch for ${stream}`);
      return { value, name: info.name };
    } finally {
      capture.active = false;
      if (--captureUsers === 0) { restoreGet!(); restoreGet = undefined; }
    }
  } finally { await nc.close(); }
};

// jetstreamManager(checkAPI=true) itself reads $JS.API.INFO. Election recovery can return the real
// JetStreamApiError {code:503, err_code:10008}; permissions/account refusal and unknown errors are fatal.
const transientRead = (error: unknown): boolean => {
  const seen = new Set<Error>();
  let transient = false;
  for (let depth = 0; depth < 8 && error instanceof Error && !seen.has(error); depth++) {
    seen.add(error);
    if (error instanceof JetStreamApiError) {
      const api = error.apiError();
      if (api.code !== 503 || api.err_code !== 10008) return false;
      transient = true;
    } else if (error.name === "TimeoutError" || error.name === "NoResponders") transient = true;
    else if (error.name !== "RequestError" && error.name !== "JetStreamNotEnabled") return false;
    if (error.cause === undefined) return transient;
    error = error.cause;
  }
  return false;
};

/** INFO has no push event for replica-current or pending NEXT. Sample only during fixture setup/recovery,
 * under one absolute deadline (including connect/requests), without publishing or resetting a consumer. */
const waitForReady = async (server: string, stream: string, consumer: string | undefined,
  timeoutMs: number): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  let last: unknown = "no stream INFO yet";
  let lastError: string | undefined;
  const expired = () => new Error(`NATS ${consumer === undefined ? "stream" : "stream/first NEXT"} readiness deadline for ${stream}: ${JSON.stringify({ last, lastError })}`);
  const bounded = async <T,>(request: () => Promise<T>): Promise<T> => {
    const remaining = deadline - Date.now();
    if (remaining <= 0) throw expired();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([request(), new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(expired()), remaining);
      })]);
    } finally { clearTimeout(timer); }
  };
  const nc = await connect({ servers: [server], ignoreClusterUpdates: true, timeout: Math.min(1_000, timeoutMs) });
  try {
    let manager: Awaited<ReturnType<typeof jetstreamManager>> | undefined;
    while (Date.now() < deadline) {
      try {
        manager ??= await bounded(() => jetstreamManager(nc, { timeout: Math.min(1_000, timeoutMs) }));
        const info = await bounded(() => manager!.streams.info(stream));
        const replicas = info.cluster?.replicas ?? [];
        const required = info.config.num_replicas;
        const leader = info.cluster?.leader;
        const streamReady = required >= 1 && replicas.length === required - 1
          && (required === 1 && info.cluster === undefined || typeof leader === "string" && leader.length > 0)
          && new Set(replicas.map(peer => peer.name)).size === replicas.length
          && replicas.every(peer => peer.name !== leader && peer.current === true && peer.offline !== true && (peer.lag ?? 0) === 0);
        const consumers = consumer === undefined ? undefined
          : [await bounded(() => manager!.consumers.info(stream, consumer))];
        last = { configuredReplicas: required, leader, replicas,
          ...(consumers === undefined ? {} : { consumers: consumers.map(item => ({ name: item.name, waiting: item.num_waiting })) }) };
        if (Date.now() < deadline && streamReady
          && (consumers === undefined || consumers.length > 0 && consumers.every(item => item.num_waiting > 0))) return;
      } catch (error) {
        lastError = error instanceof Error ? error.message : String(error);
        // Missing exact target is not ready, never evidence from another receiver. Only snapshot reads may
        // tolerate not-found; manager/account failures must match the narrow transient classifier above.
        const missing = manager !== undefined && error instanceof JetStreamApiError && error.status === 404
          && (error.code === 10059 || consumer !== undefined && error.code === 10014);
        if (Date.now() < deadline && !missing && !transientRead(error)) throw error;
      }
      const remaining = deadline - Date.now();
      if (remaining > 0) await new Promise<void>(resolve => setTimeout(resolve, Math.min(50, remaining)));
    }
    throw expired();
  } finally { await nc.close(); }
};

/** A CREATE ACK / elected leader alone does not prove all configured stream replicas have caught up. */
export const waitForNatsStreamReady = (server: string, stream: string, options: ReadinessOptions = {}): Promise<void> =>
  waitForReady(server, stream, undefined, options.timeoutMs ?? 10_000);

/** Test setup only: require full stream replication AND the broker's real pending pull before the first write.
 * Retain the existing 45s setup bound: the SDK's default pull can recover at its native 30s expiry.
 * Never use this before the deliberately paused receiver's first NEXT. */
export const waitForNatsConsumerReady = (server: string, stream: string, name: string, options: ReadinessOptions = {}): Promise<void> => {
  if (!name) return Promise.reject(new Error(`actual consumer name required for ${stream} readiness`));
  return waitForReady(server, stream, name, options.timeoutMs ?? 45_000);
};
