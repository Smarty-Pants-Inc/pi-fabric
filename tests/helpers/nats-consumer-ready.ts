import { AsyncLocalStorage } from "node:async_hooks";
import { connect } from "@nats-io/transport-node";
import { JetStreamApiError, jetstream, jetstreamManager, type Consumer, type Consumers } from "@nats-io/jetstream";

type ReadinessOptions = { timeoutMs?: number };
type StreamReadinessOptions = ReadinessOptions & { monitorUrl?: string };
type Capture = { stream: string; receivers: Consumer[]; active: boolean; restore: (() => void)[] };
type ConsumerTarget = string | Pick<Consumer, "info">;
// The pinned PullConsumerImpl exposes live stream/name even though Consumer's public type omits them.
// reset()/getConsumerOpts() advances name BEFORE the async add ACK replaces cached _info. Validate the
// runtime shape, never fall back to the predecessor cache, and treat that split as bounded not-ready.
const liveIdentity = (stream: string, consumer: Pick<Consumer, "info">): string => {
  const live = consumer as { stream?: unknown; name?: unknown };
  if (live.stream !== stream || typeof live.name !== "string" || !live.name.trim()) {
    throw new Error(`actual SDK receiver identity mismatch for ${stream}: live stream/name required`);
  }
  return live.name;
};
const identity = async (stream: string, consumer: ConsumerTarget): Promise<string | undefined> => {
  if (typeof consumer === "string") {
    if (!consumer.trim()) throw new Error(`actual SDK receiver identity mismatch for ${stream}: name required`);
    return consumer;
  }
  const name = liveIdentity(stream, consumer);
  const info = await consumer.info(true);
  const current = liveIdentity(stream, consumer);
  if (info.stream_name !== stream || typeof info.name !== "string" || !info.name.trim()) {
    throw new Error(`actual SDK receiver identity mismatch for ${stream}: cached stream/name required`);
  }
  return name === current && info.name === current ? current : undefined;
};
const captures = new AsyncLocalStorage<Capture>();
let captureUsers = 0;

const routeReadiness = (value: unknown, expectedServerId: unknown, leader: string, configuredValue: unknown): { ready: boolean; diagnostic: Record<string, unknown> } => {
  const monitor: { server_id?: unknown; server_name?: unknown; routes?: unknown } = value && typeof value === "object"
    ? value as { server_id?: unknown; server_name?: unknown; routes?: unknown } : {};
  const serverId = typeof monitor.server_id === "string" ? monitor.server_id.slice(0, 100) : undefined;
  const serverName = typeof monitor.server_name === "string" ? monitor.server_name.slice(0, 100) : undefined;
  const base = { serverId, serverName, leader };
  if (typeof monitor.server_id !== "string" || !monitor.server_id.trim()
    || typeof expectedServerId !== "string" || !expectedServerId.trim() || monitor.server_id !== expectedServerId) {
    return { ready: false, diagnostic: { ...base, expectedServerId: typeof expectedServerId === "string" ? expectedServerId.slice(0, 100) : undefined, reason: "monitor server_id mismatch" } };
  }
  if (monitor.server_name === leader) return { ready: true, diagnostic: { ...base, via: "local leader", ready: true } };

  const configured = Array.isArray(configuredValue) ? configuredValue : [];
  const patterns = configured.filter((subject): subject is string => typeof subject === "string" && subject.length > 0);
  const validSubjects = configured.length > 0 && patterns.length === configured.length;
  const routes = Array.isArray(monitor.routes) ? monitor.routes : [];
  const installed = new Set<string>();
  let leaderRoutes = 0;
  for (const value of routes) {
    if (!value || typeof value !== "object") continue;
    const route = value as { remote_name?: unknown; subscriptions_list?: unknown };
    if (route.remote_name !== leader) continue;
    leaderRoutes++;
    if (!Array.isArray(route.subscriptions_list)) continue;
    for (const subject of route.subscriptions_list) {
      if (typeof subject === "string" && subject.length > 0 && subject !== "$SYS" && !subject.startsWith("$SYS.")) installed.add(subject);
    }
  }
  const missing = patterns.filter(subject => !installed.has(subject));
  const ready = validSubjects && missing.length === 0;
  return { ready, diagnostic: { ...base, via: "remote routes", leaderRoutes, configuredSubjects: configured.length,
    installedSubjects: installed.size, missing: missing.slice(0, 8), ...(validSubjects ? {} : { reason: "empty or invalid configured subjects" }), ready } };
};
let restoreGet: (() => void) | undefined;

/** Observe the actual SDK get selected by the product, not LIST or a late ACK from an abandoned get.
 * Record only when that returned object's original consume is invoked. No extra request/await is inserted
 * between get and the product's first NEXT. Retain that SAME object: ordered SDK recovery advances its live
 * identity before its cached INFO catches up. Async-local scopes keep simultaneous same-stream opens from borrowing another receiver. */
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
  timeoutMs: number, monitorUrl?: string): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  let last: unknown = "no stream INFO yet";
  let lastError: string | undefined;
  let lastMonitor: unknown;
  const expired = () => new Error(`NATS ${consumer === undefined ? "stream" : "stream/first NEXT"} readiness deadline for ${stream}: ${JSON.stringify({ last, lastError, ...(monitorUrl === undefined ? {} : { lastMonitor }) })}`);
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
        // Resolve only this consumed handle's LIVE identity, never LIST or a different receiver.
        // A live/cache split during native delete/add is not-ready, not a fatal error or predecessor success.
        const name = consumer === undefined ? undefined : await bounded(() => identity(stream, consumer));
        const snapshot = name === undefined ? undefined : await bounded(() => manager!.consumers.info(stream, name));
        if (snapshot && (snapshot.stream_name !== stream || snapshot.name !== name)) {
          throw new Error(`consumer INFO identity mismatch for ${stream}/${name}`);
        }
        // INFO is asynchronous: an old ready ACK must not win after the handle has already advanced.
        const current = consumer === undefined ? undefined : await bounded(() => identity(stream, consumer));
        last = { configuredReplicas: required, leader, replicas,
          ...(snapshot === undefined ? {} : { consumers: [{ name: snapshot.name, waiting: snapshot.num_waiting }], current }) };
        let routesReady = true;
        if (streamReady && monitorUrl !== undefined && required > 1) {
          routesReady = false;
          lastMonitor = { leader, state: "routez pending" };
          try {
            const routez = await bounded(async () => {
              const response = await fetch(`${monitorUrl.replace(/\/+$/, "")}/routez?subs=1`);
              if (!response.ok) throw new Error(`routez HTTP ${response.status}`);
              return response.json();
            });
            const readiness = routeReadiness(routez, nc.info?.server_id, leader!, info.config.subjects);
            routesReady = readiness.ready;
            lastMonitor = readiness.diagnostic;
          } catch (error) {
            lastMonitor = { ready: false, error: (error instanceof Error ? error.message : String(error)).slice(0, 160) };
          }
        }
        if (Date.now() < deadline && streamReady && routesReady
          && (consumer === undefined || snapshot !== undefined && name === current && snapshot.num_waiting > 0)) return;
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
export const waitForNatsStreamReady = (server: string, stream: string, options: StreamReadinessOptions = {}): Promise<void> =>
  waitForReady(server, stream, undefined, options.timeoutMs ?? 10_000, options.monitorUrl);

/** Test setup only: require full stream replication AND the broker's real pending pull before the first write.
 * Retain the existing 45s setup bound: the SDK's default pull can recover at its native 30s expiry.
 * Never use this before the deliberately paused receiver's first NEXT. */
export const waitForNatsConsumerReady = (server: string, stream: string, consumer: ConsumerTarget, options: ReadinessOptions = {}): Promise<void> => {
  if (!consumer) return Promise.reject(new Error(`actual consumer identity required for ${stream} readiness`));
  return waitForReady(server, stream, consumer, options.timeoutMs ?? 45_000);
};
