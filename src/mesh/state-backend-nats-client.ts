// The real JetStreamPort of the NATS mesh state backend (smarty-dev#7504): the official nats.js v3 client
// (@nats-io/transport-node, @nats-io/jetstream). Loaded only by `openNatsStateBackend` (a lazy entry point), so the
// startup graph never includes it. The optional third-party SDK packages are imported only at the first actual
// `connectJetStream` call: a non-split bundle that flattens this module into a CLI must not gain top-level imports.
import { randomUUID } from "node:crypto";
import type { connect } from "@nats-io/transport-node";
import type { JetStreamApiError, jetstreamManager } from "@nats-io/jetstream";
import {
  assertNatsPlacementTimeout, assertNatsReplicas, DEFAULT_NATS_PLACEMENT_TIMEOUT_MS, guardNatsPort, MeshStateNatsBusyError,
  MeshStateNatsRefusedError, MeshStateNatsUnavailableError, natsConnectOptions, NatsWrongLastSequenceError, redactNatsError,
  type JetStreamPort, type NatsStreamMessage, type NatsTlsFiles,
} from "./state-backend-nats.js";

/** NATS JetStream API error codes used here. */
const WRONG_LAST_SEQUENCE = 10071;
const STREAM_NOT_FOUND = 10059;
// JSClusterNoPeersErrF: HTTP 400, "no suitable peers for placement", including an offline peer.
const CLUSTER_NO_PEERS = 10005;
// @nats-io/nats-core 3.4 names NoRespondersError "NoResponders". JetStream wraps a RequestError (whose
// cause is that transport error) in JetStreamNotEnabled; only those known wrappers may pass through classification.
const TRANSIENT = new Set(["TimeoutError", "NoResponders", "ConnectionError", "RequestError", "ClosedConnectionError"]);
const PERMANENT_TRANSPORT = new Set(["AuthorizationError", "UserAuthenticationExpiredError", "PermissionViolationError", "AuthenticationError", "PermissionError", "ProtocolError"]);
const OPAQUE_WRAPPERS = new Set(["JetStreamNotEnabled", "RequestError"]);
const READ_RETRY_ATTEMPTS = 3;
const STREAM_ADD_ATTEMPTS = 3;
const READ_RETRY_DELAY_MS = 100;
// The official JetStream API request timeout remains 5s. Read exhaustion is ~15.2s; stream creation is bounded to
// three identical add requests plus INFO reconciliation (~30.6s worst case). Placement faults instead share one
// wall-clock budget from CREATE's start, including backoff, pending requests, leader readiness and first consumer.
// Only that initial ordered-consumer get is raced at 1s per attempt: a timed-out name is abandoned, not resent.
const STREAM_LEADER_TIMEOUT_MS = 10_000;
const STREAM_LEADER_POLL_MS = 100;
const CONSUMER_CREATE_ATTEMPT_TIMEOUT_MS = 1_000;
const CONSUMER_INITIAL_INACTIVE_THRESHOLD_MS = 10_000;
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
type Bounded<T> = { kind: "value"; value: T } | { kind: "error"; error: unknown } | { kind: "deadline" };
// ponytail: reuse the existing leader-wait request race for placement, not another retry framework.
const untilDeadline = async <T>(request: () => Promise<T>, deadline: number): Promise<Bounded<T>> => {
  const remaining = deadline - Date.now();
  if (remaining <= 0) return { kind: "deadline" };
  let timer: ReturnType<typeof setTimeout> | undefined;
  return Promise.race<Bounded<T>>([
    Promise.resolve().then(async (): Promise<Bounded<T>> => {
      if (Date.now() >= deadline) return { kind: "deadline" };
      try {
        const value = await request();
        return Date.now() < deadline ? { kind: "value", value } : { kind: "deadline" };
      } catch (error) {
        return Date.now() < deadline ? { kind: "error", error } : { kind: "deadline" };
      }
    }),
    new Promise<{ kind: "deadline" }>(resolve => {
      timer = setTimeout(() => resolve({ kind: "deadline" }), remaining);
    }),
  ]).finally(() => { if (timer !== undefined) clearTimeout(timer); });
};
type NatsReadBudget = NonNullable<Parameters<JetStreamPort["getMessage"]>[2]>;
// Classification is bound to the SDK's own JetStreamApiError class (instance checks, never names), loaded lazily.
const natsErrorClassifiers = (JetStreamApiErrorClass: typeof JetStreamApiError) => {
const apiCode = (error: unknown): number | undefined => {
  if (!(error instanceof JetStreamApiErrorClass)) return undefined;
  try { return error.apiError().err_code; } catch { return error.code; }
};
const isTransientReadError = (error: unknown): boolean => {
  let current: unknown = error;
  const seen = new Set<object>();
  let transport = false;
  for (let depth = 0; depth < 8; depth++) {
    if (!(current instanceof Error)) return false;
    if (seen.has(current) || PERMANENT_TRANSPORT.has(current.name)) return false;
    seen.add(current);
    const cause = (current as Error & { cause?: unknown }).cause;
    if (current instanceof JetStreamApiErrorClass) {
      try {
        const api = current.apiError();
        if (api.err_code === 10039) return false; // JetStreamNotEnabledForAccount is permanent, not election transience.
        if (api.code !== 503) return false; // Non-503 JetStream API errors are permanent.
        transport = true;
      } catch { return false; } // malformed API errors are not retried
    } else if (TRANSIENT.has(current.name) && current.name !== "RequestError") {
      transport = true;
    } else if (!OPAQUE_WRAPPERS.has(current.name) && current.name !== "JetStreamApiError" && !TRANSIENT.has(current.name)) {
      return false;
    }
    if (cause === undefined || cause === null) return transport;
    current = cause;
  }
  return false; // The chain did not end within the fixed inspection bound.
};
// Timeout rotation must still respect the classifier's permanent-error veto and bounded wrapper inspection.
const isTransientTimeout = (error: unknown): boolean => {
  if (!isTransientReadError(error)) return false;
  let current = error;
  for (let depth = 0; depth < 8 && current instanceof Error; depth++) {
    if (current.name === "TimeoutError") return true;
    current = (current as Error & { cause?: unknown }).cause;
  }
  return false;
};
const retryTransientRead = async <T>(operation: () => Promise<T>, attempts = READ_RETRY_ATTEMPTS,
  budget?: NatsReadBudget): Promise<T> => {
  if (budget !== undefined) {
    const active = () => Date.now() < budget.deadline && budget.active?.() !== false;
    const expired = () => new MeshStateNatsBusyError("the initial projector read budget expired or was retired");
    for (let attempt = 1; ; attempt++) {
      if (!active()) throw expired();
      const outcome = await untilDeadline(() => {
        // untilDeadline defers the request: recheck authority in the actual SDK invocation closure too.
        if (!active()) throw expired();
        return operation();
      }, budget.deadline);
      if (!active() || outcome.kind === "deadline") throw expired();
      if (outcome.kind === "value") return outcome.value;
      if (!isTransientReadError(outcome.error) || attempt >= attempts) throw outcome.error;
      if (!active()) throw expired();
      const pause = await untilDeadline(() => {
        if (!active()) throw expired();
        return sleep(READ_RETRY_DELAY_MS);
      }, budget.deadline);
      if (!active() || pause.kind === "deadline") throw expired();
      if (pause.kind === "error") throw pause.error;
    }
  }
  for (let attempt = 1; ; attempt++) {
    try { return await operation(); }
    catch (error) {
      if (!isTransientReadError(error) || attempt >= attempts) throw error;
      await new Promise<void>(resolve => setTimeout(resolve, READ_RETRY_DELAY_MS));
    }
  }
};
return { apiCode, isTransientReadError, isTransientTimeout, retryTransientRead };
};

export interface ConnectJetStreamOptions {
  /** `http(s)://host:8222`: read `/varz` for `sync_always`. Host-provided only. */
  monitoringUrl?: string;
  /** Set: every connection must be TLS (these files, or the system CAs for `{}`). */
  tls?: NatsTlsFiles;
  /** The stream's replicas (positive integer, smarty-dev#11373). Omitted: the agent-layer fabric.json mesh.nats.replicas. */
  replicas?: number;
  /** One placement budget in milliseconds (default 30 000), including pending retries and leader readiness.
   * Exhausted: MeshStateNatsUnavailableError; replicas are never lowered. */
  placementTimeoutMs?: number;
}

/**
 * smarty-dev#11373: omitted replicas come only from the agent layer (`<agentDir>/fabric.json` mesh.nats.replicas, default
 * 1 when the file or value is absent; unreadable, malformed or invalid is refused), read read-only at first use, not
 * the full merged config. Dynamic imports keep this client's lazy graph unchanged; the agent dir is the host's
 * getAgentDir convention (PI_CODING_AGENT_DIR, else ~/.pi/agent) through its local mirror.
 */
const configuredReplicas = async (): Promise<number> => {
  const [{ loadAgentNatsReplicas }, { resolveAgentDir }] = await Promise.all([import("../config.js"), import("../core/agent-dir.js")]);
  return assertNatsReplicas(loadAgentNatsReplicas(resolveAgentDir()), "mesh.nats.replicas");
};

export const connectJetStream = async (servers: string[], options: ConnectJetStreamOptions = {}): Promise<JetStreamPort> => {
  if (servers.length === 0) throw new Error("NATS mesh state: no servers configured");
  const placementTimeoutMs = options.placementTimeoutMs === undefined ? DEFAULT_NATS_PLACEMENT_TIMEOUT_MS
    : assertNatsPlacementTimeout(options.placementTimeoutMs, "placementTimeoutMs");
  // Decided before any connection: an explicit value is validated, an omitted one is the agent layer's.
  const replicas = options.replicas === undefined ? await configuredReplicas() : assertNatsReplicas(options.replicas, "replicas");
  // First actual use: load the optional SDK packages now, never at module evaluation or in the background.
  const [{ connect }, { DeliverPolicy, JetStreamApiError, StorageType, jetstream, jetstreamManager }] = await Promise.all([
    import("@nats-io/transport-node"), import("@nats-io/jetstream"),
  ]);
  const { apiCode, isTransientReadError, isTransientTimeout, retryTransientRead } = natsErrorClassifiers(JetStreamApiError);
  // smarty-dev#9663 item 2: a client or fetch error may quote a server URL; never pass its credentials on.
  let nc: Awaited<ReturnType<typeof connect>> | undefined;
  let jsm: Awaited<ReturnType<typeof jetstreamManager>>;
  try {
    nc = await connect(natsConnectOptions(servers, options.tls));
    jsm = await retryTransientRead(() => jetstreamManager(nc!));
  } catch (error) {
    await nc?.drain().catch(() => undefined);
    throw redactNatsError(error);
  }
  const js = jetstream(nc);
  // Every ensure grants only its first consumer the existing readiness deadline. Freshness alone grants 10059
  // tolerance (#12900); observing an existing stream never does, even though its initial get is also bounded.
  const streamReadiness = new Map<string, { deadline: number; fresh: boolean }>();
  const bootstrapConsumer = async <T>(stream: string, request: () => Promise<T>, deadline: number,
    fresh: boolean, rotateName: () => void): Promise<T> => {
    let notFound: unknown;
    let lastError: unknown;
    let transientFailures = 0;
    const unavailable = () => new MeshStateNatsUnavailableError(notFound === undefined
      ? `stream ${stream} consumer was not created within the open's stream readiness budget; retry the open`
      : `stream ${stream} consumer could not be created within the open's stream readiness budget: JetStream err_code ` +
        `${STREAM_NOT_FOUND} (stream not found right after its creation, cluster metadata still propagating); retry the open`,
      { consumerBootstrap: true, ...(notFound === undefined ? {} : { natsErrCode: STREAM_NOT_FOUND }),
        ...(lastError === undefined ? {} : { cause: notFound ?? lastError }) });
    for (;;) {
      const bounded = await untilDeadline(request, Math.min(deadline, Date.now() + CONSUMER_CREATE_ATTEMPT_TIMEOUT_MS));
      // The shorter attempt race may expire without exhausting the shared budget. Never rotate at the overall
      // deadline (including a placement budget shorter than 1s), and never use a late abandoned get's consumer.
      const outcome = bounded.kind === "deadline" && Date.now() < deadline
        ? { kind: "attempt-timeout" as const } : bounded;
      if (outcome.kind === "deadline" || Date.now() >= deadline) throw unavailable();
      if (outcome.kind === "value") return outcome.value;
      if (outcome.kind === "attempt-timeout") rotateName();
      else {
        lastError = outcome.error;
        if (fresh && apiCode(outcome.error) === STREAM_NOT_FOUND) notFound = outcome.error;
        else if (isTransientTimeout(outcome.error)) rotateName();
        else if (!isTransientReadError(outcome.error) || ++transientFailures >= READ_RETRY_ATTEMPTS) throw outcome.error;
      }
      const remaining = deadline - Date.now();
      if (remaining <= 0) throw unavailable();
      await sleep(Math.min(STREAM_LEADER_POLL_MS, remaining));
    }
  };
  // ponytail: nats-auth scoped roles subscribe only to their own _INBOX (roleTemplate), not to
  // $JS.EVENT.ADVISORY.STREAM.LEADER_ELECTED.<stream>. The leader event is therefore unusable with the configured
  // credentials. Keep this 10s-bounded info wait until advisory access is explicitly granted (#1067 round 4 exception);
  // do not widen auth permissions merely to replace the wait with an event.
  const waitForStreamLeader = async (stream: string, requiredReplicas: number, current?: Awaited<ReturnType<typeof jsm.streams.info>>,
    budget?: { deadline: number; unavailable?: () => Error }) => {
    if (requiredReplicas === 1) return current;
    const deadline = budget?.deadline ?? Date.now() + STREAM_LEADER_TIMEOUT_MS;
    const noLeader = budget?.unavailable ?? (() => new MeshStateNatsBusyError(`stream ${stream} had no elected leader after ${STREAM_LEADER_TIMEOUT_MS}ms`));
    let info = current;
    if (info?.cluster?.leader) return info;
    for (;;) {
      const outcome = await untilDeadline(() => jsm.streams.info(stream), deadline);
      if (outcome.kind === "deadline") throw noLeader();
      if (outcome.kind === "error") {
        // The stream was created or observed already; INFO can briefly miss it while cluster state catches up.
        // Only this readiness wait treats 10059 as not ready: ordinary reads must still report a missing stream.
        if (apiCode(outcome.error) !== STREAM_NOT_FOUND && !isTransientReadError(outcome.error)) throw outcome.error;
      } else {
        info = outcome.value;
        if (Date.now() < deadline && info.cluster?.leader) return info;
      }
      const afterRequest = deadline - Date.now();
      if (afterRequest <= 0) throw noLeader();
      await sleep(Math.min(STREAM_LEADER_POLL_MS, afterRequest));
    }
  };
  const port: JetStreamPort = {
    server: async () => {
      let syncAlways: boolean | undefined;
      if (options.monitoringUrl) {
        let varz: { jetstream?: { config?: { sync_always?: unknown } } };
        try {
          const response = await fetch(new URL("/varz", options.monitoringUrl), { signal: AbortSignal.timeout(5_000) });
          varz = await response.json() as typeof varz;
        } catch (error) { throw redactNatsError(error); }
        syncAlways = typeof varz.jetstream?.config?.sync_always === "boolean" ? varz.jetstream.config.sync_always : undefined;
      }
      // smarty-dev#9690: the target identity. A normal user may ask `$SYS.REQ.USER.INFO` (measured on 2.14.7) when its
      // permissions allow it; otherwise the account stays undefined and a caller that needs it refuses.
      let account: string | undefined;
      try {
        const reply = await nc.request("$SYS.REQ.USER.INFO", "", { timeout: 2_000 });
        const info = JSON.parse(new TextDecoder().decode(reply.data)) as { data?: { account?: unknown } };
        account = typeof info.data?.account === "string" && info.data.account !== "" ? info.data.account : undefined;
      } catch { account = undefined; }
      const info = nc.info as { version?: string; server_name?: string; cluster?: string; domain?: string; max_payload?: unknown; auth_required?: unknown } | undefined;
      return { version: info?.version ?? "0.0.0", syncAlways,
        // pi-fabric#907: the payload cap and whether the server requires authentication.
        ...(typeof info?.max_payload === "number" ? { maxPayload: info.max_payload } : {}), authRequired: info?.auth_required === true,
        ...(info?.server_name ? { name: info.server_name } : {}), ...(info?.cluster ? { cluster: info.cluster } : {}),
        ...(info?.domain ? { domain: info.domain } : {}), ...(account ? { account } : {}) };
    },
    streamInfo: async (stream) => {
      try {
        const info = await retryTransientRead(() => jsm.streams.info(stream));
        return { created: String(info.created), lastSeq: info.state.last_seq, maxMsgSize: info.config.max_msg_size };
      } catch (error) { if (apiCode(error) === STREAM_NOT_FOUND) return undefined; throw error; }
    },
    createStream: async (stream, subjects, { maxMsgSize, replicas: requested }) => {
      // `streams.add` of an existing stream with the same config succeeds silently: check absence first (the caller
      // holds the migration fence, and the stream name carries a random switch id).
      let existing = false;
      try { await retryTransientRead(() => jsm.streams.info(stream)); existing = true; }
      catch (error) { if (apiCode(error) !== STREAM_NOT_FOUND) throw error; }
      if (existing) throw new MeshStateNatsRefusedError(`NATS stream ${stream} already exists`);
      // ensureStream's own create: the subjects, the server-enforced cap (pi-fabric#907), the configured replicas,
      // placement retries and leader readiness (smarty-dev#11373), and lost-reply reconciliation; never a cheap add.
      await port.ensureStream(stream, subjects, { maxMsgSize, ...(requested === undefined ? {} : { replicas: requested }) });
      const info = await retryTransientRead(() => jsm.streams.info(stream));
      return { created: String(info.created) };
    },
    deleteStream: async (stream) => {
      try { return await jsm.streams.delete(stream); }
      catch (error) { if (apiCode(error) === STREAM_NOT_FOUND) return false; throw error; }
    },
    ensureStream: async (stream, subjects, { maxMsgSize, replicas: requested, create }) => {
      streamReadiness.delete(stream); // A later ensure replaces the deadline and never inherits old freshness.
      const configured = requested === undefined ? replicas : assertNatsReplicas(requested, "replicas");
      let info;
      try { info = await retryTransientRead(() => jsm.streams.info(stream)); }
      catch (error) { if (apiCode(error) !== STREAM_NOT_FOUND) throw error; }
      if (!info && create === false) {
        // smarty-dev#9690: a root's open and a reader proof never create a stream; a migration made it.
        throw new MeshStateNatsRefusedError(`the stream ${stream} does not exist (a NATS root opens only a migrated stream)`);
      }
      if (!info) {
        // The stream itself refuses a larger message (pi-fabric#907): the server enforces the cap, not only the client.
        const config = { name: stream, subjects, storage: StorageType.File, num_replicas: configured, max_msg_size: maxMsgSize };
        let created: Awaited<ReturnType<typeof jsm.streams.add>> | undefined;
        const createStart = Date.now();
        let placementFault: unknown;
        let placementDeadline: number | undefined;
        let placementRetries = 0;
        let transientFailures = 0;
        const placementUnavailable = () => new MeshStateNatsUnavailableError(
          `stream ${stream} could not place its ${configured} replicas within ${placementTimeoutMs}ms: ` +
          `JetStream err_code ${CLUSTER_NO_PEERS} (no suitable peers for placement, peer offline or restarting); ` +
          `replicas are never lowered; restore the peers, then retry`,
          { natsErrCode: CLUSTER_NO_PEERS, cause: placementFault });
        const bounded = async <T>(request: () => Promise<T>): Promise<T> => {
          if (placementDeadline === undefined) return request();
          const outcome = await untilDeadline(request, placementDeadline);
          if (outcome.kind === "deadline") throw placementUnavailable();
          if (outcome.kind === "error") throw outcome.error;
          return outcome.value;
        };
        const pause = async (ms: number) => {
          if (placementDeadline === undefined) return sleep(ms);
          const remaining = placementDeadline - Date.now();
          if (remaining <= 0) throw placementUnavailable();
          await sleep(Math.min(ms, remaining));
        };
        for (;;) {
          try {
            created = await bounded(() => jsm.streams.add(config));
            break;
          } catch (error) {
            if (error instanceof MeshStateNatsUnavailableError) throw error;
            if (apiCode(error) === CLUSTER_NO_PEERS) {
              placementFault = error;
              placementDeadline ??= createStart + placementTimeoutMs;
              await pause(Math.min(2_000, 250 * 2 ** Math.min(placementRetries++, 3)));
              continue;
            }
            if (!isTransientReadError(error)) throw error;
            // STREAM.CREATE is idempotent only when its reply was lost and the exact requested config is already
            // present. Reconcile before another identical request, so a stored stream is never mistaken for failure.
            let observed: Awaited<ReturnType<typeof jsm.streams.info>> | undefined;
            try { observed = await bounded(() => jsm.streams.info(stream)); }
            catch (readError) {
              if (readError instanceof MeshStateNatsUnavailableError) throw readError;
              if (apiCode(readError) !== STREAM_NOT_FOUND && !isTransientReadError(readError)) throw readError;
            }
            if (observed) {
              const actualSubjects = [...(observed.config.subjects ?? [])].sort();
              const expectedSubjects = [...subjects].sort();
              if (observed.config.num_replicas !== configured || observed.config.storage !== StorageType.File ||
                  observed.config.max_msg_size !== maxMsgSize || actualSubjects.length !== expectedSubjects.length ||
                  actualSubjects.some((subject, index) => subject !== expectedSubjects[index])) {
                throw new MeshStateNatsRefusedError(`stream ${stream} exists after an ambiguous create but its storage, subjects, replicas, or max_msg_size differ from the requested configuration`);
              }
              info = observed;
              break;
            }
            if (++transientFailures >= STREAM_ADD_ATTEMPTS) throw error;
            await pause(READ_RETRY_DELAY_MS);
          }
        }
        if (!created && !info) throw new Error(`NATS mesh state: stream ${stream} create returned no stream info`);
        const readiness = placementDeadline === undefined ? { deadline: Date.now() + STREAM_LEADER_TIMEOUT_MS }
          : { deadline: placementDeadline, unavailable: placementUnavailable };
        const ready = await waitForStreamLeader(stream, configured, created ?? info, readiness);
        streamReadiness.set(stream, { deadline: readiness.deadline, fresh: true });
        return { lastSeq: ready?.state.last_seq ?? (created ?? info)!.state.last_seq,
          maxMsgSize: ready?.config.max_msg_size ?? (created ?? info)!.config.max_msg_size };
      }
      // smarty-dev#11373: an existing stream with other replicas is refused BEFORE any update (or a later publish or
      // purge): never lowered, never raised automatically. The operator changes the stream or the configuration.
      const observed = typeof info.config.num_replicas === "number" ? info.config.num_replicas : Number.NaN;
      if (observed !== configured) {
        throw new MeshStateNatsRefusedError(`the stream ${stream} has ${observed} replicas but ${configured} are configured ` +
          "(mesh.nats.replicas); it is never changed automatically", { configured, observed });
      }
      const missing = subjects.filter(subject => !(info.config.subjects ?? []).includes(subject));
      // `create: false` never changes a stream either: a migrated stream already has both subjects.
      if (missing.length > 0 && create === false) {
        throw new MeshStateNatsRefusedError(`the stream ${stream} lacks the subjects ${missing.join(", ")} (a NATS root opens only a migrated stream)`);
      }
      if (missing.length > 0) await jsm.streams.update(stream, { subjects: [...(info.config.subjects ?? []), ...missing] });
      const readiness = { deadline: Date.now() + STREAM_LEADER_TIMEOUT_MS };
      const ready = await waitForStreamLeader(stream, configured, info, readiness);
      streamReadiness.set(stream, { deadline: readiness.deadline, fresh: false });
      return { lastSeq: ready?.state.last_seq ?? info.state.last_seq, maxMsgSize: ready?.config.max_msg_size ?? info.config.max_msg_size };
    },
    streamExists: async (stream) => {
      try { await retryTransientRead(() => jsm.streams.info(stream)); return true; }
      catch (error) { if (apiCode(error) === STREAM_NOT_FOUND) return false; throw error; }
    },
    lastSequence: async (stream) => (await retryTransientRead(() => jsm.streams.info(stream))).state.last_seq,
    firstSequence: async (stream) => (await retryTransientRead(() => jsm.streams.info(stream))).state.first_seq,
    getMessage: async (stream, seq, budget) => {
      const message = await retryTransientRead(() => jsm.streams.getMessage(stream, { seq }), READ_RETRY_ATTEMPTS, budget);
      const msgID = message?.header.get("Nats-Msg-Id");
      return message ? { seq: message.seq, data: message.data, ...(msgID ? { msgID } : {}) } : undefined;
    },
    lastBySubject: async (stream, subject) => {
      const message = await retryTransientRead(() => jsm.streams.getMessage(stream, { last_by_subj: subject }));
      const msgID = message?.header.get("Nats-Msg-Id");
      return message ? { seq: message.seq, data: message.data, ...(msgID ? { msgID } : {}) } : undefined;
    },
    purge: async (stream, beforeSeq) => { await jsm.streams.purge(stream, { seq: beforeSeq }); },
    publish: async (subject, data, { expectLastSequence, msgID }) => {
      try {
        const ack = await js.publish(subject, data, { msgID, expect: { lastSequence: expectLastSequence } });
        return { seq: ack.seq, duplicate: ack.duplicate };
      } catch (error) {
        if (apiCode(error) === WRONG_LAST_SEQUENCE) throw new NatsWrongLastSequenceError(expectLastSequence, (error as Error).message);
        // No answer (the ack may be lost): the backend retries with the same id. Anything else is final.
        // (nats-core's error classes, by name: @nats-io/transport-node does not re-export them.)
        if (error instanceof Error && isTransientReadError(error)) throw Object.assign(error, { transient: true });
        throw error;
      }
    },
    consume: async (stream, afterSeq, onMessage: (message: NatsStreamMessage) => void, onError) => {
      // Taken before the first request: second consumes, resyncs and running consumers get no missing-stream retry.
      const readiness = streamReadiness.get(stream);
      streamReadiness.delete(stream);
      let consumerOptions = {
        name_prefix: randomUUID(),
        inactive_threshold: CONSUMER_INITIAL_INACTIVE_THRESHOLD_MS,
        ...(afterSeq === 0
          ? { deliver_policy: DeliverPolicy.All }
          : { deliver_policy: DeliverPolicy.StartSequence, opt_start_seq: afterSeq + 1 }),
      };
      const request = () => js.consumers.get(stream, consumerOptions);
      // Keep the SDK client's default 5s API timeout: its API persists into the running consumer. The race only
      // caps initial get. Each Timeout abandons our own ephemeral assignment; do not delete it or consume it later.
      // Replace, rather than mutate, its options so an in-flight SDK get retains only its abandoned name.
      const consumer = readiness === undefined ? await retryTransientRead(request, READ_RETRY_ATTEMPTS)
        : await bootstrapConsumer(stream, request, readiness.deadline, readiness.fresh,
          () => { consumerOptions = { ...consumerOptions, name_prefix: randomUUID() }; });
      const messages = await consumer.consume();
      void (async () => { for await (const message of messages) onMessage({ seq: message.seq, data: message.data }); })()
        .catch(error => onError?.(error));
      // smarty-dev#9690: a stopped projector also deletes its (ephemeral) consumer at once, best effort, so a proof or a
      // closed store leaves nothing on the server; the server removes a missed one after its inactive threshold.
      return { stop: async () => {
        await messages.close();
        // Best effort; a consumer without delete (a test double) has nothing to remove.
        if (typeof consumer.delete === "function") await consumer.delete().catch(() => false);
      } };
    },
    close: async () => { await nc.drain(); },
  };
  return guardNatsPort(port);
};
