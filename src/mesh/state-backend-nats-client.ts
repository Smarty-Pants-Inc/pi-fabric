// The real JetStreamPort of the NATS mesh state backend (smarty-dev#7504): the official nats.js v3 client
// (@nats-io/transport-node, @nats-io/jetstream). Loaded only by `openNatsStateBackend` (a lazy entry point), so the
// startup graph never includes it.
import { connect } from "@nats-io/transport-node";
import {
  DeliverPolicy, JetStreamApiError, StorageType, jetstream, jetstreamManager,
} from "@nats-io/jetstream";
import { NatsWrongLastSequenceError, type JetStreamPort, type NatsStreamMessage } from "./state-backend-nats.js";

/** NATS JetStream API error codes used here. */
const WRONG_LAST_SEQUENCE = 10071;
const STREAM_NOT_FOUND = 10059;
const TRANSIENT = new Set(["TimeoutError", "NoRespondersError", "ConnectionError", "RequestError", "ClosedConnectionError"]);
const apiCode = (error: unknown): number | undefined => {
  if (!(error instanceof JetStreamApiError)) return undefined;
  try { return error.apiError().err_code; } catch { return error.code; }
};

export interface ConnectJetStreamOptions {
  /** `http(s)://host:8222`: read `/varz` for `sync_always`. Host-provided only. */
  monitoringUrl?: string;
}

export const connectJetStream = async (servers: string[], options: ConnectJetStreamOptions = {}): Promise<JetStreamPort> => {
  if (servers.length === 0) throw new Error("NATS mesh state: no servers configured");
  const nc = await connect({ servers });
  const js = jetstream(nc);
  const jsm = await jetstreamManager(nc);
  return {
    server: async () => {
      let syncAlways: boolean | undefined;
      if (options.monitoringUrl) {
        const response = await fetch(new URL("/varz", options.monitoringUrl), { signal: AbortSignal.timeout(5_000) });
        const varz = await response.json() as { jetstream?: { config?: { sync_always?: unknown } } };
        syncAlways = typeof varz.jetstream?.config?.sync_always === "boolean" ? varz.jetstream.config.sync_always : undefined;
      }
      return { version: nc.info?.version ?? "0.0.0", syncAlways };
    },
    ensureStream: async (stream, subject) => {
      try { return { lastSeq: (await jsm.streams.info(stream)).state.last_seq }; }
      catch (error) { if (apiCode(error) !== STREAM_NOT_FOUND) throw error; }
      const created = await jsm.streams.add({ name: stream, subjects: [subject], storage: StorageType.File, num_replicas: 1 });
      return { lastSeq: created.state.last_seq };
    },
    lastSequence: async (stream) => (await jsm.streams.info(stream)).state.last_seq,
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
  };
};
