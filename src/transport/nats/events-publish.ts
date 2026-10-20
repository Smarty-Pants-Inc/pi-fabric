import { Buffer } from "node:buffer";
import { isDeepStrictEqual } from "node:util";
import { JetStreamApiError, type PubAck } from "@nats-io/jetstream";
import type { MeshEvent } from "../../mesh/event-log.js";
import type { NatsConnectionHandle } from "./connection.js";
import { ensureNatsEventsStream, natsEventsStreamName, type NatsEventsStreamOptions } from "./provision.js";

const WRONG_LAST_SEQUENCE = 10071;
const MAX_EVENT_MESSAGE_BYTES = 1_048_576;
const PUBLISH_ATTEMPT_TIMEOUT_MS = 1_000;
const MAX_PUBLISH_ATTEMPTS = 8;
const TRANSIENT = new Set(["TimeoutError", "NoResponders", "ConnectionError", "ClosedConnectionError"]);
const PERMANENT_TRANSPORT = new Set(["AuthorizationError", "UserAuthenticationExpiredError", "PermissionViolationError", "AuthenticationError", "PermissionError", "ProtocolError"]);
const OPAQUE_WRAPPERS = new Set(["JetStreamNotEnabled", "RequestError"]);

const apiCode = (error: unknown): number | undefined => {
  if (!(error instanceof JetStreamApiError)) return undefined;
  try { return error.apiError().err_code; } catch { return error.code; }
};

// Same bounded, permanent-error-vetoed classifier as state-backend-nats-client. Do not import that backend's
// runtime graph just to classify a publish: it brings state projection, filesystem storage and config with it.
const isTransient = (error: unknown): boolean => {
  let current = error;
  let transport = false;
  const seen = new Set<Error>();
  for (let depth = 0; depth < 8; depth++) {
    if (!(current instanceof Error) || seen.has(current) || PERMANENT_TRANSPORT.has(current.name)) return false;
    seen.add(current);
    if (current instanceof JetStreamApiError) {
      try {
        const api = current.apiError();
        if (api.err_code === 10039 || api.code !== 503) return false;
        transport = true;
      } catch { return false; }
    } else if (TRANSIENT.has(current.name)) transport = true;
    else if (!OPAQUE_WRAPPERS.has(current.name)) return false;
    const cause: unknown = current.cause;
    if (cause === undefined || cause === null) return transport;
    current = cause;
  }
  return false;
};

/** Resolved Fabric envelope: the caller owns id, timestamps, admission and provenance; JetStream owns sequence. */
export type NatsEventPublishInput = Omit<MeshEvent, "sequence">;

export interface NatsEventsPublisherOptions extends NatsEventsStreamOptions {
  /** Budget for CAS / same-ID publish attempts (default 5s); final read-back uses the connection's API timeout. */
  readonly publishTimeoutMs?: number;
}

export interface NatsEventPublishResult {
  readonly event: MeshEvent;
  readonly duplicate: boolean;
}

export interface NatsEventsPublisher {
  readonly org: string;
  readonly stream: string;
  lastSequence(): Promise<number>;
  publish(event: NatsEventPublishInput): Promise<MeshEvent>;
  publishWithAck(event: NatsEventPublishInput): Promise<NatsEventPublishResult>;
}

/** No ambiguous publish remains: the caller may retry its fixed envelope after contention subsides. */
export class NatsEventsPublishBusyError extends Error {
  readonly code = "FABRIC_NATS_EVENTS_BUSY";
  readonly retryable = true;
  constructor() {
    super("NATS events publish attempt budget exhausted; retry the same event id");
    this.name = "NatsEventsPublishBusyError";
  }
}

/** Never turn a lost answer / missing recovery record into permission to assign a fresh event id. */
export class NatsEventsPublishOutcomeUnknownError extends Error {
  readonly code = "FABRIC_NATS_EVENTS_OUTCOME_UNKNOWN";
  constructor(readonly eventId: string, options?: ErrorOptions) {
    super(`NATS event ${eventId} may be stored; reconcile before publishing again`, options);
    this.name = "NatsEventsPublishOutcomeUnknownError";
  }
}

export class NatsEventsIdConflictError extends Error {
  readonly code = "FABRIC_NATS_EVENTS_ID_CONFLICT";
  constructor(readonly eventId: string) {
    super(`NATS event id ${eventId} is already stored with different event content`);
    this.name = "NatsEventsIdConflictError";
  }
}

const validSequence = (sequence: number): boolean => Number.isSafeInteger(sequence) && sequence >= 0;
const sameEventIdentity = (left: MeshEvent, right: MeshEvent): boolean => {
  if (!Number.isFinite(left.createdAt) || !Number.isFinite(right.createdAt)) return false;
  // A caller may retry the same id/body in a new CLI invocation with a fresh timestamp. The
  // first stored timestamp remains authoritative; every other envelope field must still match.
  const stable = (event: MeshEvent): Record<string, unknown> => {
    const value = { ...event } as Record<string, unknown>;
    delete value.createdAt;
    return value;
  };
  return isDeepStrictEqual(stable(left), stable(right));
};
const topicSubject = (org: string, topic: string): string => {
  // Preserve topic literally (including dots, slashes and colons). Wildcards, whitespace and empty NATS tokens
  // cannot be published exactly, so refuse rather than hash, escape or silently rewrite a Fabric topic.
  if (typeof topic !== "string" || /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/.exec(topic)?.[0] !== topic ||
    topic.split(".").some(token => token.length === 0)) throw new Error("Invalid NATS Fabric event topic");
  return `fabric.ev.${org}.${topic}`;
};

/**
 * Uses an already-open connection; never dials, closes it, creates consumers, or replays history. Pass the host's
 * mesh.nats.provision explicitly to opt into stream creation. Otherwise opening only checks the existing stream.
 */
export const openNatsEventsPublisher = async (
  connection: Pick<NatsConnectionHandle, "js" | "manager">,
  options: NatsEventsPublisherOptions,
): Promise<NatsEventsPublisher> => {
  const publishTimeoutMs = options.publishTimeoutMs ?? 5_000;
  if (!Number.isSafeInteger(publishTimeoutMs) || publishTimeoutMs <= 0) throw new Error("NATS events publishTimeoutMs must be a positive safe integer");
  const org = options.org;
  const stream = natsEventsStreamName(org);
  const { js, manager } = connection;
  const info = await ensureNatsEventsStream(manager, options);
  // The stream may be server-unbounded (max_msg_size: -1); enforce a hard 1 MiB application cap anyway.
  const maxMessageBytes = Math.min(options.maxMessageBytes ?? MAX_EVENT_MESSAGE_BYTES, MAX_EVENT_MESSAGE_BYTES);
  const lastSequence = async (): Promise<number> => {
    const sequence = (await manager.streams.info(stream)).state.last_seq;
    if (!validSequence(sequence)) throw new Error("Invalid NATS events stream sequence");
    return sequence;
  };

  const publishWithAck = async (input: NatsEventPublishInput): Promise<NatsEventPublishResult> => {
      const deadline = performance.now() + publishTimeoutMs;
      // Capture once before any await: caller mutation, data getters or a toJSON must not change a retry's bytes.
      const captured = JSON.parse(JSON.stringify(input)) as MeshEvent;
      delete (captured as Partial<MeshEvent>).sequence;
      const id = captured.id;
      if (typeof id !== "string" || id.length === 0 || id !== id.trim() || /[\u0000-\u001f\u007f]/.test(id)) {
        throw new Error("NATS Fabric event id must be a nonempty, header-safe literal");
      }
      const subject = topicSubject(org, captured.topic);
      const unknown = (cause?: unknown): NatsEventsPublishOutcomeUnknownError =>
        new NatsEventsPublishOutcomeUnknownError(id, cause === undefined ? undefined : { cause });
      let base = await lastSequence();
      const prepare = (): { event: MeshEvent; data: Uint8Array } => {
        if (base === Number.MAX_SAFE_INTEGER) throw new Error("NATS events stream sequence exhausted its safe integer range");
        const event = { ...captured, sequence: base + 1 };
        const data = Buffer.from(JSON.stringify(event), "utf8");
        if (data.byteLength > maxMessageBytes) throw new Error(`NATS Fabric event exceeds ${maxMessageBytes} bytes`);
        return { event, data };
      };
      let candidate = prepare();
      let lostAck = false;

      // The CAS can have stored our attempt only at base+1. A foreign message there positively rules it out,
      // even if our timed-out request arrives late. An absent/unreadable record does NOT establish non-publication.
      const readBack = async (sequence: number): Promise<MeshEvent | undefined> => {
        let stored;
        try { stored = await manager.streams.getMessage(stream, { seq: sequence }); }
        catch (cause) { throw unknown(cause); }
        if (!stored || stored.seq !== sequence) throw unknown();
        if (stored.header.get("Nats-Msg-Id") !== id) return undefined;
        let event: MeshEvent;
        try { event = JSON.parse(Buffer.from(stored.data).toString("utf8")) as MeshEvent; }
        catch (cause) { throw unknown(cause); }
        if (stored.subject !== subject || !sameEventIdentity(event, { ...candidate.event, sequence })) {
          throw new NatsEventsIdConflictError(id);
        }
        return event;
      };

      for (let attempt = 0; ; attempt++) {
        const remaining = deadline - performance.now();
        if (remaining < 1 || attempt >= MAX_PUBLISH_ATTEMPTS) {
          if (lostAck) {
            const committed = await readBack(base + 1);
            if (committed) return { event: committed, duplicate: true };
          }
          throw new NatsEventsPublishBusyError();
        }
        let ack: PubAck;
        try {
          ack = await js.publish(subject, candidate.data, {
            msgID: id,
            expect: { streamName: stream, lastSequence: base },
            timeout: Math.min(remaining, PUBLISH_ATTEMPT_TIMEOUT_MS),
          });
        } catch (error) {
          if (apiCode(error) === WRONG_LAST_SEQUENCE) {
            if (lostAck) {
              const committed = await readBack(base + 1);
              if (committed) return { event: committed, duplicate: true };
            }
            // Only a definite CAS refusal (plus foreign-record evidence after a lost ACK) permits rebasing.
            base = await lastSequence();
            candidate = prepare();
            lostAck = false;
            continue;
          }
          if (isTransient(error)) {
            lostAck = true;
            continue; // Exactly the same bytes, msgID and expectation; dedupe precedes CAS on nats-server.
          }
          if (lostAck) {
            const committed = await readBack(base + 1);
            if (committed) return { event: committed, duplicate: true };
          }
          throw error; // Permissions, invalid payloads and capacity refusals are final, never blindly retried.
        }
        if (ack.stream !== stream || !validSequence(ack.seq) || ack.seq === 0) throw unknown();
        if (ack.duplicate) {
          // A retry by the caller may have observed a newer head; return the ORIGINAL event/sequence, not the
          // speculative sequence just encoded. Never treat a deduped id with different content as success.
          const committed = await readBack(ack.seq);
          if (!committed) throw unknown();
          return { event: committed, duplicate: true };
        }
        if (ack.seq !== base + 1) throw unknown();
        return { event: candidate.event, duplicate: ack.duplicate };
      }
    };
  return {
    org,
    stream,
    lastSequence,
    async publish(input) { return (await publishWithAck(input)).event; },
    publishWithAck,
  };
};

/** Store integration entry. event.sequence is speculative and is replaced with the committed stream sequence.
 * The stream must already exist; creation is exclusively ensureFabricEventsStream(manager, org, provision).
 * Callers publishing repeatedly may retain openNatsEventsPublisher(connection, { org }) instead.
 */
export const publishFabricEvent = async (
  connection: Pick<NatsConnectionHandle, "js" | "manager">,
  org: string,
  event: MeshEvent,
): Promise<MeshEvent> => (await openNatsEventsPublisher(connection, { org })).publish(event);
