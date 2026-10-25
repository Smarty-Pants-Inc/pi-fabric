// A' bridge, NATS event-stream consume leg (smarty-dev#15310, lane L1; APRIME2 N4/T14, PR 2).
//
// v2 Mains publish topic events to FABRIC_EV_<ORG> (`fabric.ev.<org>.<topic>`), never to the file
// mesh. This leg reads the allow-listed subjects with one durable and hands each event, with the
// sender identity taken from KV presence, to L2's file-publish half (route rules, rate bounds,
// the durable file append). It ACKs only when that half reports the append durable.
//
// Trust boundary (residual, for L3's threat model): an event subject carries NO source machine,
// and the Path B events grant lets every events-granted machine publish any `fabric.ev.<org>.>`
// subject (permissions.ts "Shared org authority, NOT actor isolation"). So `from.id` cannot be
// bound to the publishing credential. This leg requires `from.id` to resolve to exactly one LIVE
// KV root on a machine other than the bridge machine, and takes the identity (name, kind,
// sessionId) from that KV record, never from the body. A machine holding the events grant can
// still claim another live root's id. Nothing that already crossed a bridge (`data.bridge`)
// crosses again, and nothing that claims a file identity this link mirrors comes back.
import { Buffer } from "node:buffer";
// Static on purpose (scripts/build.mjs refuses unstable package-local dynamic chunks); nothing in the
// startup graph imports this module.
import { AckPolicy, DeliverPolicy, JetStreamApiError, ReplayPolicy, type ConsumerMessages } from "@nats-io/jetstream";
import { natsEventsStreamName } from "../transport/nats/provision.js";
import type { MeshEvent, MeshIdentity } from "./event-log.js";
import type { BridgeNatsPortResolved } from "./bridge-nats-types.js";
import type { NatsConnectionHandle } from "../transport/nats/connection.js";

const ORG = /^[a-z0-9][a-z0-9-]{0,62}$/;
const MACHINE_TOKEN = /^[a-z0-9][a-z0-9-]{0,31}$/;
const TOPIC = /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/;
/** A filter is an exact topic or a topic prefix ending in `.>` (the whole remainder). No `*`. */
const FILTER = /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,125}(?:\.>)?$/;
export const NATS_BRIDGE_EVENT_RETRY_MS = [1_000, 5_000, 15_000, 30_000] as const;

export type NatsBridgeEventDisposition = "accepted" | "retry" | "refused";

export type NatsBridgeEventRefusal =
  | "BRIDGE_NATS_EVENT_MALFORMED"
  | "BRIDGE_NATS_BRIDGED_AGAIN"
  | "BRIDGE_NATS_SENDER_UNVERIFIED"
  | "BRIDGE_NATS_SENDER_IS_BRIDGE";

/** The JetStream message surface the leg needs (a subset of JsMsg). */
export interface NatsBridgeEventMessage {
  readonly subject: string;
  readonly data: Uint8Array;
  readonly headers?: { get(name: string): string } | undefined;
  readonly info: { readonly streamSequence: number; readonly deliveryCount: number };
  ackAck(): Promise<boolean>;
  nak(delayMs?: number): void;
  term(reason?: string): void;
}

export interface NatsBridgeEventSource {
  consume(signal: AbortSignal): AsyncIterable<NatsBridgeEventMessage>;
  isVerified(): boolean;
}

/** One admitted event: the stream's sequence, its message id, and the sender as KV presence names it. */
export interface NatsBridgeEventDelivery {
  /** The event as published, `sequence` replaced by the stream sequence; `from` is the VERIFIED identity. */
  readonly event: MeshEvent;
  /** The body's claimed `from`, for logs only. */
  readonly claimedFrom: MeshIdentity;
  /** Nats-Msg-Id (the publisher's event id) or the stream sequence: the dedupe key across a restart. */
  readonly messageId: string;
  readonly streamSequence: number;
  readonly attempt: number;
  /** The KV machine of the sender's live presence row. */
  readonly senderMachine: string;
}

export interface NatsBridgeEventsOptions {
  readonly org: string;
  /** The bridge's own machine: a sender live there is one of this link's mirrors, never a NATS sender. */
  readonly machine: string;
  /** Topic filters under `fabric.ev.<org>.`: exact topics or `prefix.>` (e.g. `fleet.work.>`, `ops.owner`). */
  readonly topics: readonly string[];
  readonly source: NatsBridgeEventSource;
  /** The port's resolve(): the one KV record for an id, or undefined when absent or ambiguous. */
  readonly resolve: (id: string) => BridgeNatsPortResolved | undefined;
  /**
   * L2's file-publish half: apply the file bridge's route rules and rate bounds, then append. Return
   * "accepted" only once the append is durable (or the event was already appended, by messageId);
   * "refused" for a route refusal (TERM, never redelivered); "retry" to NAK and see it again.
   */
  readonly onEvent: (delivery: NatsBridgeEventDelivery) => Promise<NatsBridgeEventDisposition>;
  readonly onRefused?: (code: NatsBridgeEventRefusal, detail: { subject: string; messageId?: string; fromId?: string }) => void;
  readonly onError?: (error: unknown) => void;
  readonly retryDelayScheduleMs?: readonly number[];
}

export const natsBridgeEventSubjects = (org: string, topics: readonly string[]): string[] => {
  if (!ORG.test(org) || org === "sys") throw new TypeError("Invalid NATS events org");
  if (!Array.isArray(topics) || topics.length === 0) throw new TypeError("NATS bridge events need at least one topic filter");
  return [...new Set(topics.map(topic => {
    if (typeof topic !== "string" || !FILTER.test(topic) || topic.split(".").some(token => token.length === 0)) {
      throw new TypeError(`Invalid NATS bridge event topic filter: ${String(topic)}`);
    }
    return `fabric.ev.${org}.${topic}`;
  }))].sort();
};

const matchesFilter = (subject: string, filters: readonly string[]): boolean =>
  filters.some(filter => filter.endsWith(".>") ? subject.startsWith(filter.slice(0, -1)) && subject.length > filter.length - 1 : subject === filter);

const isIdentity = (value: unknown): value is MeshIdentity => typeof value === "object" && value !== null &&
  typeof (value as MeshIdentity).id === "string" && (value as MeshIdentity).id.length > 0 &&
  typeof (value as MeshIdentity).name === "string" && ["main", "actor", "agent"].includes((value as MeshIdentity).kind);

/** Strict decode, the same envelope rules as the actor events consumer (events-consume.ts decodeEvent). */
export const decodeNatsBridgeEvent = (org: string, message: Pick<NatsBridgeEventMessage, "subject" | "data" | "headers" | "info">):
  { event: MeshEvent; messageId: string } => {
  let value: unknown;
  try { value = JSON.parse(Buffer.from(message.data).toString("utf8")); } catch { throw new Error("invalid NATS MeshEvent JSON"); }
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error("invalid NATS MeshEvent");
  const event = value as MeshEvent;
  if (typeof event.id !== "string" || event.id.length === 0 || typeof event.topic !== "string" || !TOPIC.test(event.topic) ||
    event.topic.split(".").some(token => token.length === 0) || typeof event.kind !== "string" || !isIdentity(event.from) ||
    !Number.isFinite(event.createdAt) || message.subject !== `fabric.ev.${org}.${event.topic}`) {
    throw new Error("invalid NATS MeshEvent envelope or subject");
  }
  const sequence = message.info.streamSequence;
  if (!Number.isSafeInteger(sequence) || sequence <= 0) throw new Error("invalid NATS event stream sequence");
  const headerId = message.headers?.get("Nats-Msg-Id");
  return { event: { ...event, sequence }, messageId: typeof headerId === "string" && headerId.trim().length > 0 ? headerId : String(sequence) };
};

/**
 * Run the consume leg until `signal` aborts. Events are handled one at a time in stream order; a
 * thrown onEvent is a retry. Settling is suspended while the connection awaits re-verification.
 */
export const runNatsBridgeEvents = async (options: NatsBridgeEventsOptions, signal: AbortSignal): Promise<void> => {
  if (!MACHINE_TOKEN.test(options.machine)) throw new TypeError("Invalid NATS bridge machine");
  const filters = natsBridgeEventSubjects(options.org, options.topics);
  const schedule = (options.retryDelayScheduleMs ?? NATS_BRIDGE_EVENT_RETRY_MS).map(ms => Math.max(1, Math.min(30_000, Math.floor(ms))));
  if (schedule.length === 0 || schedule.some(ms => !Number.isSafeInteger(ms))) throw new TypeError("Invalid NATS bridge event retry schedule");
  const delay = (attempt: number): number => schedule[Math.min(Math.max(1, attempt) - 1, schedule.length - 1)]!;
  const refuse = (code: NatsBridgeEventRefusal, detail: { subject: string; messageId?: string; fromId?: string }): void => {
    try { options.onRefused?.(code, detail); } catch { /* diagnostic only */ }
  };
  for await (const message of options.source.consume(signal)) {
    if (signal.aborted) break;
    if (!options.source.isVerified()) continue; // Never admit or settle while USER.INFO is pending.
    const settleTerm = (reason: string): void => { if (options.source.isVerified()) message.term(reason); };
    if (!matchesFilter(message.subject, filters)) { message.nak(delay(message.info.deliveryCount)); continue; }
    let decoded: ReturnType<typeof decodeNatsBridgeEvent>;
    try { decoded = decodeNatsBridgeEvent(options.org, message); }
    catch {
      refuse("BRIDGE_NATS_EVENT_MALFORMED", { subject: message.subject });
      settleTerm("invalid NATS MeshEvent envelope");
      continue;
    }
    const { event, messageId } = decoded;
    const data = typeof event.data === "object" && event.data !== null && !Array.isArray(event.data) ? event.data as Record<string, unknown> : undefined;
    if (data && Object.hasOwn(data, "bridge")) {
      refuse("BRIDGE_NATS_BRIDGED_AGAIN", { subject: message.subject, messageId, fromId: event.from.id });
      settleTerm("already bridged");
      continue;
    }
    let sender: BridgeNatsPortResolved | undefined;
    try { sender = options.resolve(event.from.id); }
    catch (error) {
      // An unknown view (no KV snapshot yet) is not a refusal: see the event again later.
      if ((error as { code?: unknown } | null)?.code !== "FABRIC_DIRECTORY_NOT_READY") throw error;
      message.nak(delay(message.info.deliveryCount));
      continue;
    }
    if (sender && sender.route.machine === options.machine) {
      // A file identity this link mirrors (or the link itself) can only be spoken for by this link.
      refuse("BRIDGE_NATS_SENDER_IS_BRIDGE", { subject: message.subject, messageId, fromId: event.from.id });
      settleTerm("sender is a bridge mirror");
      continue;
    }
    if (!sender || !sender.route.live || sender.identity.kind !== "main" || sender.identity.id !== event.from.id) {
      refuse("BRIDGE_NATS_SENDER_UNVERIFIED", { subject: message.subject, messageId, fromId: event.from.id });
      settleTerm("sender is not a live NATS root");
      continue;
    }
    const delivery: NatsBridgeEventDelivery = {
      event: { ...event, from: { ...sender.identity } }, claimedFrom: event.from, messageId,
      streamSequence: message.info.streamSequence, attempt: message.info.deliveryCount, senderMachine: sender.route.machine,
    };
    let disposition: NatsBridgeEventDisposition;
    try { disposition = await options.onEvent(delivery); }
    catch (error) {
      try { options.onError?.(error); } catch { /* diagnostic only */ }
      disposition = "retry";
    }
    if (!options.source.isVerified()) continue; // The broker redelivers after ack_wait; nothing is lost.
    try {
      if (disposition === "accepted") {
        if (!await message.ackAck()) throw new Error("NATS bridge event ACK was not confirmed");
      } else if (disposition === "refused") message.term("refused by the bridge route rules");
      else message.nak(delay(message.info.deliveryCount));
    } catch (error) {
      try { options.onError?.(error); } catch { /* diagnostic only */ }
    }
  }
};

/**
 * The real source: one durable (`bev_<hash(link identity)>`) on FABRIC_EV_<ORG> over the filters,
 * explicit ACK, unlimited redelivery with backoff, one message in flight (stream order end to end).
 * A new durable starts at the stream's NEW messages (or `startAt`), never replaying history into
 * Ryzen 1. Needs a connection verified WITH the events grant (events: true, org).
 */
/**
 * The one events durable of a bridge machine, named under the machine so the org can grant exactly it
 * (`$JS.API.CONSUMER.{CREATE,INFO,MSG.NEXT}.FABRIC_EV_<ORG>.bev_<machine>`, `$JS.ACK.FABRIC_EV_<ORG>.bev_<machine>.>`).
 */
export const natsBridgeEventsDurable = (machine: string): string => {
  if (!/^[a-z0-9][a-z0-9-]{0,31}$/.test(machine)) throw new TypeError("Invalid NATS bridge machine");
  return `bev_${machine}`;
};

const CONSUMER_NOT_FOUND = 10014;
const apiErrorCode = (error: unknown): number | undefined => {
  if (!(error instanceof JetStreamApiError)) return undefined;
  try { return error.apiError().err_code; } catch { return undefined; }
};

export interface NatsBridgeEventSourceOptions {
  readonly org: string;
  /** The bridge's own machine: it names the durable. */
  readonly machine: string;
  readonly topics: readonly string[];
  /** Only when the durable is created; default: new messages only, never replaying history into the hub. */
  readonly startAt?: number | string | Date;
  readonly retryDelayScheduleMs?: readonly number[];
}

/**
 * Read-only on the stream: look the durable up and create it if absent. Nothing here publishes, and
 * nothing creates, updates or deletes a stream. A durable with other settings is refused, never changed.
 */
export const ensureNatsBridgeEventConsumer = async (connection: Pick<NatsConnectionHandle, "manager">,
  options: NatsBridgeEventSourceOptions): Promise<{ stream: string; durable: string }> => {
  const filters = natsBridgeEventSubjects(options.org, options.topics);
  const schedule = options.retryDelayScheduleMs ?? NATS_BRIDGE_EVENT_RETRY_MS;
  const stream = natsEventsStreamName(options.org);
  const durable = natsBridgeEventsDurable(options.machine);
  const api = connection.manager.consumers;
  let info;
  try { info = await api.info(stream, durable); }
  catch (error) {
    if (apiErrorCode(error) !== CONSUMER_NOT_FOUND) throw error;
    const startTime = options.startAt === undefined ? undefined : new Date(options.startAt).toISOString();
    try {
      info = await api.add(stream, {
        filter_subjects: filters, ack_policy: AckPolicy.Explicit, replay_policy: ReplayPolicy.Instant,
        ack_wait: 60_000 * 1_000_000, max_deliver: -1, max_ack_pending: 1,
        backoff: [...schedule, schedule[schedule.length - 1]!].map(ms => ms * 1_000_000), inactive_threshold: 0,
        durable_name: durable, deliver_policy: startTime === undefined ? DeliverPolicy.New : DeliverPolicy.StartTime,
        ...(startTime === undefined ? {} : { opt_start_time: startTime }),
      });
    } catch (createError) {
      // A concurrent opener may have created it; never reset an existing cursor.
      info = await api.info(stream, durable).catch(() => { throw createError; });
    }
  }
  if (info.config.ack_policy !== AckPolicy.Explicit || info.config.deliver_subject ||
    JSON.stringify([...(info.config.filter_subjects ?? [])].sort()) !== JSON.stringify(filters)) {
    throw Object.assign(new Error(`Incompatible NATS bridge events consumer: ${durable}`), { code: "FABRIC_TRANSPORT_INVALID" });
  }
  return { stream, durable };
};

export const natsBridgeEventSource = (connection: NatsConnectionHandle, options: NatsBridgeEventSourceOptions): NatsBridgeEventSource => {
  natsBridgeEventSubjects(options.org, options.topics);
  return {
    isVerified: () => connection.isVerified(),
    async *consume(signal) {
      const { stream, durable } = await ensureNatsBridgeEventConsumer(connection, options);
      const consumer = await connection.js.consumers.get(stream, durable);
      const messages: ConsumerMessages = await consumer.consume({ max_messages: 1, abort_on_missing_resource: true });
      const stop = (): void => { void messages.close().catch(() => undefined); };
      signal.addEventListener("abort", stop, { once: true });
      try {
        for await (const message of messages) {
          if (signal.aborted) break;
          yield message as unknown as NatsBridgeEventMessage;
        }
        const error = await messages.closed();
        if (error && !signal.aborted) throw error;
      } finally {
        signal.removeEventListener("abort", stop);
        await messages.close().catch(() => undefined);
      }
    },
  };
};
