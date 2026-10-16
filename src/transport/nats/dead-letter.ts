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
import type { MeshIdentity } from "../../mesh/event-log.js";
import type { TransportEnvelope } from "../types.js";
import { PATH_B_ADVISORY_DURABLE } from "../../mesh/nats-auth/permissions.js";
import {
  ADVISORY_CAPTURE_STREAM,
  transportAdvisoryStreamSubject,
  transportDurableName,
  transportInboxStream,
  transportInboxSubject,
  validateMachine,
} from "./subjects.js";

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
  readonly originalSubject?: string;
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
  return { stream, consumer, streamSeq, ...(deliveries === undefined ? {} : { deliveries }), raw };
};

export const deadLetterId = (advisory: Pick<MaxDeliverAdvisory, "stream" | "consumer" | "streamSeq">): string =>
  `dead-letter:${advisory.stream}:${advisory.consumer}:${advisory.streamSeq}`;

export interface DeadLetterPublisherOptions {
  readonly js: JetStreamClient;
  /** E16b role identity; defaults to fabric-ops, may have a lowercase alphanumeric suffix. Never an execution host. */
  readonly machine?: string;
  readonly machines: readonly string[];
  readonly timeoutMs?: number;
}

interface OriginalRoute {
  readonly envelope: Pick<TransportEnvelope, "id" | "kind" | "to" | "targetId">;
  readonly sourceMachine: string;
  readonly replyTo?: string;
  readonly commandId?: string;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);
const nonempty = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const isIdentity = (value: unknown): value is MeshIdentity => isRecord(value) && nonempty(value.id) &&
  typeof value.name === "string" && (value.kind === "main" || value.kind === "actor" || value.kind === "agent");

/** The stored subject, not JSON sourceMachine/from, is the authenticated return-machine stamp. */
function originalRoute(letter: DeadLetterEnvelope, machines: ReadonlySet<string>): OriginalRoute {
  if (!letter.original || !letter.originalSubject) throw new Error("original unavailable");
  const wire: unknown = JSON.parse(Buffer.from(letter.original).toString("utf8"));
  if (!isRecord(wire) || wire.format !== 1 || !isRecord(wire.envelope)) throw new Error("invalid transport wire");
  const envelope = wire.envelope;
  const to = envelope.to;
  const parts = letter.originalSubject.split(".");
  const sourceMachine = parts[6];
  if (parts.length !== 7 || parts[0] !== "fabric" || parts[1] !== "in" || !sourceMachine ||
    !machines.has(sourceMachine) || wire.sourceMachine !== sourceMachine || !isRecord(to) ||
    !nonempty(to.machine) || !machines.has(to.machine) || !nonempty(to.hostId) ||
    transportInboxStream(to.machine) !== letter.stream || transportDurableName(to.hostId) !== letter.consumer ||
    !nonempty(envelope.id) || !nonempty(envelope.targetId) || !nonempty(envelope.operation) ||
    !isIdentity(envelope.from) || typeof envelope.createdAt !== "number" || !Number.isFinite(envelope.createdAt) ||
    (envelope.kind !== "command" && envelope.kind !== "ack") ||
    letter.originalSubject !== transportInboxSubject(to.machine, to.hostId, envelope.kind === "command" ? "cmd" : "ack", envelope.targetId, sourceMachine)) {
    throw new Error("original subject/envelope mismatch");
  }
  const validated: OriginalRoute["envelope"] = { id: envelope.id, kind: envelope.kind, to: { machine: to.machine, hostId: to.hostId }, targetId: envelope.targetId };
  if (envelope.kind === "ack") return { envelope: validated, sourceMachine };
  const body = envelope.body;
  if (!isRecord(body) || body.version !== 1 || !nonempty(body.commandId) || body.targetId !== envelope.targetId ||
    body.operation !== envelope.operation || !nonempty(body.replyTo)) throw new Error("invalid command return route");
  // E14.1/2: the transport id identifies this request event; commandId is logical application dedupe.
  return { envelope: validated, sourceMachine, replyTo: body.replyTo, commandId: body.commandId };
}

/**
 * Operator-only sink. All publications must receive a PubAck before capture ACKs the advisory.
 * Stable message IDs reconcile retries within the configured broker duplicate window (10 min).
 * Public records contain references/diagnostics, never the original private command payload.
 */
export function createDeadLetterPublisher(options: DeadLetterPublisherOptions): DeadLetterSink {
  const machine = validateMachine(options.machine ?? "fabric-ops");
  const operatorNamespace = /^fabric-ops(-[a-z0-9]+)?$/;
  if (!operatorNamespace.test(machine)) throw new Error("dead-letter publisher requires a fabric-ops namespace operator identity");
  const machines = new Set(options.machines.map(validateMachine));
  if (machines.size === 0 || [...machines].some((runtime) => operatorNamespace.test(runtime))) {
    throw new Error("dead-letter publisher requires a runtime inventory excluding the fabric-ops namespace");
  }
  const streams = new Set([...machines].map(transportInboxStream));
  const from: MeshIdentity = { id: `transport-operator:${machine}`, name: "fabric-transport-admin", kind: "main" };
  const publish = async (subject: string, value: unknown, id: string, streamName: string): Promise<void> => {
    await options.js.publish(subject, Buffer.from(JSON.stringify(value)), {
      msgID: id,
      expect: { streamName },
      timeout: options.timeoutMs ?? 5_000,
    });
  };
  return {
    async put(letter): Promise<void> {
      if (!streams.has(letter.stream) || !/^rt_[a-f0-9]{32}$/.test(letter.consumer) ||
        !Number.isSafeInteger(letter.streamSequence) || letter.streamSequence <= 0 ||
        letter.id !== deadLetterId({ stream: letter.stream, consumer: letter.consumer, streamSeq: letter.streamSequence })) {
        throw new Error("dead-letter record is outside the machine inventory or has an invalid identity");
      }
      let route: OriginalRoute | undefined;
      try { route = originalRoute(letter, machines); } catch { /* durable alarm, but never invent a return route */ }
      const disposition = route?.replyTo ? "outcome-unknown" : route ? "ack-exhausted" : letter.originalExpired ? "original-unavailable" : "invalid-original";
      const createdAt = Date.now();
      const data = {
        stream: letter.stream, consumer: letter.consumer, streamSequence: letter.streamSequence,
        deliveries: letter.deliveries, originalExpired: letter.originalExpired, disposition,
        ...(route ? { ownerHostId: route.envelope.to.hostId, targetId: route.envelope.targetId, envelopeId: route.envelope.id } : {}),
      };
      const event = (id: string, topic: string) => ({
        format: 1, sourceMachine: machine, id, topic, kind: "dead-letter", from, createdAt, data,
      });
      await publish(`fabric.ev.${machine}.fabric.ops.dlq`, event(letter.id, "fabric.ops.dlq"), letter.id, "FABRIC_EV");
      const alarmId = `${letter.id}:alarm`;
      await publish(`fabric.ev.${machine}.fabric.ops.owner.alarm`, event(alarmId, "fabric.ops.owner.alarm"), alarmId, "FABRIC_EV");
      if (route?.replyTo) {
        const id = `relay:${route.envelope.id}`;
        const envelope: TransportEnvelope & { readonly createdAt: number } = {
          id, kind: "ack", to: { machine: route.sourceMachine, hostId: route.replyTo },
          targetId: route.envelope.targetId, operation: "rejected", from, createdAt,
          body: {
            version: 1, commandId: route.commandId, commandEventId: route.envelope.id, targetId: route.envelope.targetId, accepted: false,
            error: "Relay destination unavailable", errorCode: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", retryable: true,
          },
        };
        await publish(transportInboxSubject(route.sourceMachine, route.replyTo, "ack", route.envelope.targetId, machine),
          { format: 1, sourceMachine: machine, envelope }, id, transportInboxStream(route.sourceMachine));
      }
    },
  };
}

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
    consumer.config.filter_subjects?.length || consumer.config.ack_policy !== AckPolicy.Explicit ||
    consumer.config.ack_wait !== 30 * 1_000_000_000 || consumer.config.max_deliver !== -1 || consumer.config.max_ack_pending !== 128 ||
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
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.All,
      replay_policy: ReplayPolicy.Instant,
      ack_wait: 30 * 1_000_000_000,
      // A failed disposition must remain recoverable; do not dead-letter the operator's own work.
      max_deliver: -1,
      max_ack_pending: 128,
    });
    await readDeadLetterConsumer(manager);
  }
}

/** Bind the fixed bootstrap-owned durable; persist/reconcile each disposition before ACK. */
export async function consumeDeadLetters(options: DeadLetterOperatorOptions): Promise<number> {
  const streams = new Set(options.machines.map(transportInboxStream));
  if (streams.size === 0) throw new Error("dead-letter operator requires a machine inventory");
  options.signal.throwIfAborted();
  await readDeadLetterConsumer(options.manager);
  const messages = await (await options.js.consumers.get(ADVISORY_CAPTURE_STREAM, PATH_B_ADVISORY_DURABLE)).consume({ max_messages: options.once ? 1 : 16 });
  let count = 0;
  const stop = (): void => { void messages.close(); };
  options.signal.addEventListener("abort", stop, { once: true });
  try {
    if (options.signal.aborted) return 0;
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
        ...(original == null ? {} : { original: Uint8Array.from(original.data), originalSubject: original.subject }),
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
