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
import { ADVISORY_CAPTURE_STREAM, transportAdvisoryStreamSubject, transportInboxStream } from "./subjects.js";

export interface MaxDeliverAdvisory {
  readonly stream: string;
  readonly consumer: string;
  readonly streamSeq: number;
  readonly deliveries?: number;
  readonly raw: unknown;
}

export interface DeadLetterEnvelope {
  readonly id: string;
  readonly stream: string;
  readonly consumer: string;
  readonly streamSequence: number;
  readonly deliveries?: number;
  readonly original?: Uint8Array;
  readonly originalExpired: boolean;
  readonly advisory: unknown;
}

export interface DeadLetterSink {
  /** Must durably persist or idempotently reconcile before the advisory is acked. */
  put(envelope: DeadLetterEnvelope): Promise<void>;
}

const parseAdvisory = (msg: JsMsg, streams: ReadonlySet<string>): MaxDeliverAdvisory => {
  let raw: unknown;
  try { raw = JSON.parse(Buffer.from(msg.data).toString("utf8")); } catch { throw new Error("invalid MAX_DELIVERIES advisory JSON"); }
  if (raw === null || typeof raw !== "object") throw new Error("invalid MAX_DELIVERIES advisory");
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
  return { stream, consumer, streamSeq, ...(deliveries === undefined ? {} : { deliveries }), raw };
};

export const deadLetterId = (advisory: Pick<MaxDeliverAdvisory, "stream" | "consumer" | "streamSeq">): string =>
  `dead-letter:${advisory.stream}:${advisory.consumer}:${advisory.streamSeq}`;

export interface DeadLetterOperatorOptions {
  readonly manager: JetStreamManager;
  readonly js: JetStreamClient;
  readonly durable?: string;
  readonly signal: AbortSignal;
  readonly sink: DeadLetterSink;
  /** Literal Path B inventory; never trust advisory JSON to select an arbitrary stream. */
  readonly machines: readonly string[];
  readonly once?: boolean;
}

/** Consume durable MAX_DELIVERIES advisories and persist one dedupable envelope before acking. */
export async function consumeDeadLetters(options: DeadLetterOperatorOptions): Promise<number> {
  const durable = options.durable ?? "transport_dlq_operator";
  const streams = new Set(options.machines.map(transportInboxStream));
  if (streams.size === 0) throw new Error("dead-letter operator requires a machine inventory");
  let consumer;
  try {
    consumer = await options.manager.consumers.info(ADVISORY_CAPTURE_STREAM, durable);
  } catch (error) {
    if (!(error instanceof JetStreamApiError) || error.apiError().err_code !== 10014) throw error;
    consumer = await options.manager.consumers.add(ADVISORY_CAPTURE_STREAM, {
      durable_name: durable,
      filter_subject: transportAdvisoryStreamSubject,
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.All,
      replay_policy: ReplayPolicy.Instant,
      ack_wait: 30 * 1_000_000_000,
      max_deliver: 5,
      max_ack_pending: 128,
    });
  }
  if (consumer.config.filter_subject !== transportAdvisoryStreamSubject || consumer.config.ack_policy !== AckPolicy.Explicit ||
    consumer.config.ack_wait !== 30 * 1_000_000_000 || consumer.config.max_deliver !== 5 || consumer.config.max_ack_pending !== 128 ||
    consumer.config.deliver_policy !== DeliverPolicy.All || consumer.config.deliver_subject) throw new Error("FABRIC_ADV consumer configuration drift");
  const messages = await (await options.js.consumers.get(ADVISORY_CAPTURE_STREAM, durable)).consume({ max_messages: options.once ? 1 : 16 });
  let count = 0;
  const stop = (): void => { void messages.close(); };
  options.signal.addEventListener("abort", stop, { once: true });
  try {
    for await (const msg of messages) {
      if (options.signal.aborted) break;
      let advisory: MaxDeliverAdvisory;
      try { advisory = parseAdvisory(msg, streams); }
      catch { await msg.term("malformed MAX_DELIVERIES advisory"); continue; }
      let original;
      try { original = await options.manager.streams.getMessage(advisory.stream, { seq: advisory.streamSeq }); }
      catch (error) {
        if (!(error instanceof JetStreamApiError) || error.apiError().err_code !== 10037) throw error;
      }
      const envelope: DeadLetterEnvelope = {
        id: deadLetterId(advisory),
        stream: advisory.stream,
        consumer: advisory.consumer,
        streamSequence: advisory.streamSeq,
        ...(advisory.deliveries === undefined ? {} : { deliveries: advisory.deliveries }),
        ...(original == null ? {} : { original: Uint8Array.from(original.data) }),
        originalExpired: original == null,
        advisory: advisory.raw,
      };
      await options.sink.put(envelope);
      const confirmed = await msg.ackAck();
      if (!confirmed) throw new Error(`MAX_DELIVERIES advisory ack was not confirmed for ${envelope.id}`);
      count += 1;
      if (options.once) break;
    }
  } finally {
    options.signal.removeEventListener("abort", stop);
    await messages.close();
  }
  return count;
}
