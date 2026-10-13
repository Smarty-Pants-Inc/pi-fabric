// The real JetStreamPort of the NATS mesh state backend (smarty-dev#7504): the official nats.js v3 client
// (@nats-io/transport-node, @nats-io/jetstream). Loaded only by `openNatsStateBackend` (a lazy entry point), so the
// startup graph never includes it.
import { connect } from "@nats-io/transport-node";
import {
  DeliverPolicy, JetStreamApiError, StorageType, jetstream, jetstreamManager,
} from "@nats-io/jetstream";
import {
  assertNatsReplicas, guardNatsPort, MeshStateNatsBusyError, MeshStateNatsRefusedError, natsConnectOptions, NatsWrongLastSequenceError, redactNatsError,
  type JetStreamPort, type NatsStreamMessage, type NatsTlsFiles,
} from "./state-backend-nats.js";

/** NATS JetStream API error codes used here. */
const WRONG_LAST_SEQUENCE = 10071;
const STREAM_NOT_FOUND = 10059;
const TRANSIENT = new Set(["TimeoutError", "NoRespondersError", "ConnectionError", "RequestError", "ClosedConnectionError"]);
const STREAM_LEADER_TIMEOUT_MS = 10_000;
const STREAM_LEADER_POLL_MS = 100;
const apiCode = (error: unknown): number | undefined => {
  if (!(error instanceof JetStreamApiError)) return undefined;
  try { return error.apiError().err_code; } catch { return error.code; }
};
const isTransientReadError = (error: unknown): boolean => {
  if (error instanceof Error && TRANSIENT.has(error.name)) return true;
  if (!(error instanceof JetStreamApiError)) return false;
  try { return error.apiError().code === 503; } catch { return false; }
};

export interface ConnectJetStreamOptions {
  /** `http(s)://host:8222`: read `/varz` for `sync_always`. Host-provided only. */
  monitoringUrl?: string;
  /** Set: every connection must be TLS (these files, or the system CAs for `{}`). */
  tls?: NatsTlsFiles;
  /** The stream's replicas (positive integer, smarty-dev#11373). Omitted: the agent-layer fabric.json mesh.nats.replicas. */
  replicas?: number;
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
  // Decided before any connection: an explicit value is validated, an omitted one is the agent layer's.
  const replicas = options.replicas === undefined ? await configuredReplicas() : assertNatsReplicas(options.replicas, "replicas");
  // smarty-dev#9663 item 2: a client or fetch error may quote a server URL; never pass its credentials on.
  let nc: Awaited<ReturnType<typeof connect>>;
  let jsm: Awaited<ReturnType<typeof jetstreamManager>>;
  try {
    nc = await connect(natsConnectOptions(servers, options.tls));
    jsm = await jetstreamManager(nc);
  } catch (error) { throw redactNatsError(error); }
  const js = jetstream(nc);
  // ponytail: nats-auth scoped roles subscribe only to their own _INBOX (roleTemplate), not to
  // $JS.EVENT.ADVISORY.STREAM.LEADER_ELECTED.<stream>. The leader event is therefore unusable with the configured
  // credentials. Keep this 10s-bounded info wait until advisory access is explicitly granted (#1067 round 4 exception);
  // do not widen auth permissions merely to replace the wait with an event.
  const waitForStreamLeader = async (stream: string, requiredReplicas: number, current?: Awaited<ReturnType<typeof jsm.streams.info>>) => {
    if (requiredReplicas === 1) return current;
    const deadline = Date.now() + STREAM_LEADER_TIMEOUT_MS;
    let info = current;
    if (info?.cluster?.leader) return info;
    for (;;) {
      try {
        info = await jsm.streams.info(stream);
        if (info.cluster?.leader) return info;
      } catch (error) {
        if (!isTransientReadError(error)) throw error;
      }
      const remaining = deadline - Date.now();
      if (remaining <= 0) {
        throw new MeshStateNatsBusyError(`stream ${stream} had no elected leader after ${STREAM_LEADER_TIMEOUT_MS}ms`);
      }
      await new Promise<void>(resolve => setTimeout(resolve, Math.min(STREAM_LEADER_POLL_MS, remaining)));
    }
  };
  return guardNatsPort({
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
      return { version: nc.info?.version ?? "0.0.0", syncAlways,
        ...(typeof nc.info?.max_payload === "number" ? { maxPayload: nc.info.max_payload } : {}),
        authRequired: nc.info?.auth_required === true };
    },
    ensureStream: async (stream, subjects, { maxMsgSize, replicas: requested }) => {
      const configured = requested === undefined ? replicas : assertNatsReplicas(requested, "replicas");
      let info;
      try { info = await jsm.streams.info(stream); }
      catch (error) { if (apiCode(error) !== STREAM_NOT_FOUND) throw error; }
      if (!info) {
        // The stream itself refuses a larger message (pi-fabric#907): the server enforces the cap, not only the client.
        const created = await jsm.streams.add({ name: stream, subjects, storage: StorageType.File, num_replicas: configured, max_msg_size: maxMsgSize });
        const ready = await waitForStreamLeader(stream, configured, created);
        return { lastSeq: ready?.state.last_seq ?? created.state.last_seq, maxMsgSize: ready?.config.max_msg_size ?? created.config.max_msg_size };
      }
      // smarty-dev#11373: an existing stream with other replicas is refused BEFORE any update (or a later publish or
      // purge): never lowered, never raised automatically. The operator changes the stream or the configuration.
      const observed = typeof info.config.num_replicas === "number" ? info.config.num_replicas : Number.NaN;
      if (observed !== configured) {
        throw new MeshStateNatsRefusedError(`the stream ${stream} has ${observed} replicas but ${configured} are configured ` +
          "(mesh.nats.replicas); it is never changed automatically", { configured, observed });
      }
      const missing = subjects.filter(subject => !(info.config.subjects ?? []).includes(subject));
      if (missing.length > 0) await jsm.streams.update(stream, { subjects: [...(info.config.subjects ?? []), ...missing] });
      const ready = await waitForStreamLeader(stream, configured, info);
      return { lastSeq: ready?.state.last_seq ?? info.state.last_seq, maxMsgSize: ready?.config.max_msg_size ?? info.config.max_msg_size };
    },
    streamExists: async (stream) => {
      try { await jsm.streams.info(stream); return true; }
      catch (error) { if (apiCode(error) === STREAM_NOT_FOUND) return false; throw error; }
    },
    lastSequence: async (stream) => (await jsm.streams.info(stream)).state.last_seq,
    firstSequence: async (stream) => (await jsm.streams.info(stream)).state.first_seq,
    getMessage: async (stream, seq) => {
      const message = await jsm.streams.getMessage(stream, { seq });
      return message ? { seq: message.seq, data: message.data } : undefined;
    },
    lastBySubject: async (stream, subject) => {
      const message = await jsm.streams.getMessage(stream, { last_by_subj: subject });
      return message ? { seq: message.seq, data: message.data } : undefined;
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
        if (error instanceof Error && TRANSIENT.has(error.name)) throw Object.assign(error, { transient: true });
        throw error;
      }
    },
    consume: async (stream, afterSeq, onMessage: (message: NatsStreamMessage) => void, onError) => {
      const consumer = await js.consumers.get(stream, afterSeq === 0
        ? { deliver_policy: DeliverPolicy.All }
        : { deliver_policy: DeliverPolicy.StartSequence, opt_start_seq: afterSeq + 1 });
      const messages = await consumer.consume();
      void (async () => { for await (const message of messages) onMessage({ seq: message.seq, data: message.data }); })()
        .catch(error => onError?.(error));
      return { stop: async () => { await messages.close(); } };
    },
    close: async () => { await nc.drain(); },
  });
};
