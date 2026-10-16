import { Buffer } from "node:buffer";
import {
  AckPolicy,
  DeliverPolicy,
  ReplayPolicy,
  type JetStreamClient,
  type JetStreamManager,
  JetStreamApiError,
} from "@nats-io/jetstream";
import type { JsMsg } from "@nats-io/jetstream";
import { PATH_B_ADVISORY_DURABLE } from "../../mesh/nats-auth/permissions.js";
import {
  ADVISORY_CAPTURE_STREAM,
  transportAdvisoryStreamSubject,
  transportInboxStream,
} from "./subjects.js";

/** E19 report-only metadata. No original message, advisory extras, or inferred disposition. */
export interface DeadLetterEnvelope {
  readonly stream: string;
  readonly consumer: string;
  readonly stream_seq: number;
  readonly deliveries: number;
}

export interface DeadLetterSink {
  /** AckNone advances on delivery: a sink failure cannot cause broker redelivery. */
  put(envelope: DeadLetterEnvelope): Promise<void>;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);

const parseAdvisory = (msg: JsMsg, streams: ReadonlySet<string>): DeadLetterEnvelope => {
  let raw: unknown;
  try { raw = JSON.parse(Buffer.from(msg.data).toString("utf8")); } catch { throw new Error("invalid MAX_DELIVERIES advisory JSON"); }
  if (!isRecord(raw)) throw new Error("invalid MAX_DELIVERIES advisory");
  const value = raw as Record<string, unknown>;
  const stream = typeof value.stream === "string" ? value.stream : undefined;
  const consumer = typeof value.consumer === "string" ? value.consumer : undefined;
  const streamSeq = value.stream_seq;
  const deliveries = typeof value.deliveries === "number" ? value.deliveries : undefined;
  if (!stream || !streams.has(stream) || !consumer || !/^rt_[a-f0-9]{32}$/.test(consumer) ||
    typeof streamSeq !== "number" || !Number.isSafeInteger(streamSeq) || streamSeq <= 0 ||
    value.type !== "io.nats.jetstream.advisory.v1.max_deliver" ||
    deliveries === undefined || !Number.isSafeInteger(deliveries) || deliveries < 5 ||
    msg.subject !== `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.${stream}.${consumer}`) {
    throw new Error("invalid MAX_DELIVERIES advisory fields");
  }
  return { stream, consumer, stream_seq: streamSeq, deliveries };
};

export interface DeadLetterOperatorOptions {
  readonly manager: JetStreamManager;
  readonly js: JetStreamClient;
  readonly signal: AbortSignal;
  readonly sink: DeadLetterSink;
  /** Literal Path B inventory; never trust advisory JSON to select an arbitrary stream. */
  readonly machines: readonly string[];
  readonly once?: boolean;
}

async function readDeadLetterConsumer(manager: JetStreamManager) {
  const consumer = await manager.consumers.info(ADVISORY_CAPTURE_STREAM, PATH_B_ADVISORY_DURABLE);
  if (consumer.config.durable_name !== PATH_B_ADVISORY_DURABLE || consumer.config.filter_subject !== transportAdvisoryStreamSubject ||
    consumer.config.filter_subjects?.length || consumer.config.ack_policy !== AckPolicy.None ||
    consumer.config.deliver_policy !== DeliverPolicy.All || consumer.config.replay_policy !== ReplayPolicy.Instant ||
    consumer.config.deliver_subject || consumer.config.headers_only || consumer.config.inactive_threshold) {
    throw new Error("FABRIC_ADV consumer configuration drift");
  }
  return consumer;
}

/** Bootstrap-only provisioning. The operator's credential has no consumer lifecycle authority. */
export async function ensureDeadLetterConsumer(manager: JetStreamManager): Promise<void> {
  try { await readDeadLetterConsumer(manager); }
  catch (error) {
    if (!(error instanceof JetStreamApiError) || error.apiError().err_code !== 10014) throw error;
    await manager.consumers.add(ADVISORY_CAPTURE_STREAM, {
      durable_name: PATH_B_ADVISORY_DURABLE,
      filter_subject: transportAdvisoryStreamSubject,
      ack_policy: AckPolicy.None,
      deliver_policy: DeliverPolicy.All,
      replay_policy: ReplayPolicy.Instant,
    });
    await readDeadLetterConsumer(manager);
  }
}

/**
 * Bind only. AckNone records advance when taken, not when persisted: a crash may lose a local
 * capture, though Limits retention keeps the advisory in FABRIC_ADV. Counts are not records.
 * Finite single pulls avoid consume()'s speculative refill, including for --once/malformed data.
 */
export async function consumeDeadLetters(options: DeadLetterOperatorOptions): Promise<number> {
  const streams = new Set(options.machines.map(transportInboxStream));
  if (streams.size === 0) throw new Error("dead-letter operator requires a machine inventory");
  options.signal.throwIfAborted();
  await readDeadLetterConsumer(options.manager);
  const consumer = await options.js.consumers.get(ADVISORY_CAPTURE_STREAM, PATH_B_ADVISORY_DURABLE);
  let count = 0;
  do {
    if (options.signal.aborted) break;
    const messages = await consumer.fetch({ max_messages: 1, expires: 1_000 });
    const stop = (): void => { void messages.close(); };
    options.signal.addEventListener("abort", stop, { once: true });
    try {
      if (options.signal.aborted) break;
      for await (const msg of messages) {
        if (options.signal.aborted) break;
        let advisory: DeadLetterEnvelope;
        try { advisory = parseAdvisory(msg, streams); }
        catch { continue; } // No ACK, NAK or TERM, even for malformed/untrusted data.
        await options.sink.put(advisory);
        count += 1;
      }
    } finally {
      options.signal.removeEventListener("abort", stop);
      await messages.close();
    }
  } while (!options.once && !options.signal.aborted);
  return count;
}
