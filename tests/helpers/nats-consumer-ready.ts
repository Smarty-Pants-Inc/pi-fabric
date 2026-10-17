import { AsyncLocalStorage } from "node:async_hooks";
import { connect } from "@nats-io/transport-node";
import { JetStreamApiError, jetstream, jetstreamManager, type Consumer, type Consumers } from "@nats-io/jetstream";

type ReadinessOptions = { timeoutMs?: number };
type Capture = { stream: string; receivers: Consumer[]; active: boolean; restore: (() => void)[] };
type ConsumerTarget = string | Pick<Consumer, "info">;
const identity = async (stream: string, consumer: ConsumerTarget): Promise<string> => {
  const info = typeof consumer === "string" ? { stream_name: stream, name: consumer } : await consumer.info(true);
  if (info.stream_name !== stream || typeof info.name !== "string" || !info.name.trim()) {
    throw new Error(`actual SDK receiver identity mismatch for ${stream}: stream/name required`);
  }
  return info.name;
};
const captures = new AsyncLocalStorage<Capture>();
let captureUsers = 0;
let restoreGet: (() => void) | undefined;

/** Observe the actual SDK get selected by the product, not LIST or a late ACK from an abandoned get.
 * Record only when that returned object's original consume is invoked. No extra request/await is inserted
 * between get and the product's first NEXT. Retain that SAME object: ordered SDK recovery changes its cached
 * identity in place. Async-local scopes keep simultaneous same-stream opens from borrowing another receiver. */
export const captureNatsConsumer = async <T,>(server: string, stream: string, operation: () => Promise<T>): Promise<{ value: T; consumer: Consumer }> => {
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
          const observed: Consumer["consume"] = function (...options: Parameters<Consumer["consume"]>) {
            receiver.consume = consume;
            if (capture.active) capture.receivers.push(receiver);
            return consume.apply(receiver, options);
          };
          receiver.consume = observed;
          capture.restore.push(() => { if (receiver.consume === observed) receiver.consume = consume; });
        }
        return receiver;
      };
      restoreGet = () => { prototype.get = get; };
    }
    captureUsers++;
    const capture: Capture = { stream, receivers: [], active: true, restore: [] };
    try {
      const value = await captures.run(capture, operation);
      if (capture.receivers.length !== 1) throw new Error(`expected one actual SDK receiver for ${stream}, observed ${capture.receivers.length}`);
      const consumer = capture.receivers[0]!;
      await identity(stream, consumer);
      return { value, consumer };
    } finally {
      capture.active = false;
      for (const restore of capture.restore) restore();
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
const waitForReady = async (server: string, stream: string, consumer: ConsumerTarget | undefined,
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
        // Resolve only this consumed handle's current cached identity, never LIST or a different receiver.
        // Native delete/add may leave its predecessor absent until the same object exposes its successor.
        const name = consumer === undefined ? undefined : await bounded(() => identity(stream, consumer));
        const snapshot = name === undefined ? undefined : await bounded(() => manager!.consumers.info(stream, name));
        if (snapshot && (snapshot.stream_name !== stream || snapshot.name !== name)) {
          throw new Error(`consumer INFO identity mismatch for ${stream}/${name}`);
        }
        // INFO is asynchronous: an old ready ACK must not win after the handle has already advanced.
        const current = consumer === undefined ? undefined : await bounded(() => identity(stream, consumer));
        last = { configuredReplicas: required, leader, replicas,
          ...(snapshot === undefined ? {} : { consumers: [{ name: snapshot.name, waiting: snapshot.num_waiting }], current }) };
        if (Date.now() < deadline && streamReady
          && (snapshot === undefined || name === current && snapshot.num_waiting > 0)) return;
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
export const waitForNatsConsumerReady = (server: string, stream: string, consumer: ConsumerTarget, options: ReadinessOptions = {}): Promise<void> => {
  if (!consumer) return Promise.reject(new Error(`actual consumer identity required for ${stream} readiness`));
  return waitForReady(server, stream, consumer, options.timeoutMs ?? 45_000);
};
