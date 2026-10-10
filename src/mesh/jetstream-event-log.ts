import { createHash, randomUUID } from "node:crypto";
import path from "node:path";
import {
  AckPolicy, connect, DeliverPolicy, DiscardPolicy, nanos, ReplayPolicy, RetentionPolicy, StorageType,
  type Consumer, type ConsumerMessages, type JsMsg, type JetStreamClient, type JetStreamManager,
  type NatsConnection, type StreamConfig,
} from "nats";
import { copyFabricPrincipal } from "../fabric-provenance.js";
import type { MeshEvent, MeshPublishInput, MeshTailResult } from "./event-log.js";
import { JetStreamPublishUncertainError, JetStreamCursorExpiredError, JetStreamBatchPublishError,
  type EventLogReadOptions, type MeshEventLogBackend } from "./event-backend.js";

const TOPIC_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/;
const hash = (text: string): string => createHash("sha256").update(text).digest("hex");
const clone = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;
const topicCheck = (topic: string): void => {
  if (!TOPIC_PATTERN.test(topic)) throw new Error(`Invalid Fabric mesh topic: ${topic}`);
};
const cursorCheck = (cursor: number): void => {
  if (!Number.isSafeInteger(cursor) || cursor < 0 || cursor === Number.MAX_SAFE_INTEGER) throw new Error("Invalid mesh sequence cursor");
};
const apiCode = (error: unknown): number | undefined =>
  (error as { api_error?: { err_code?: number } } | null)?.api_error?.err_code;

export interface JetStreamEventLogOptions {
  root: string;
  servers: string | string[];
  /** Stable shared root identity when different hosts mount the same root at different paths. */
  rootId?: string;
  replicas?: 1 | 3;
  maxEventBytes?: number;
  maxReadEvents?: number;
  /** Live log: bounded bytes. Archive: unlimited, as today's MeshArchive. */
  retention?: "live" | "archive";
  maxBytes?: number;
  /** 0 (default) means no age expiry: today's mesh archive has no age collector. */
  maxAgeMs?: number;
  /** Exactly-once publication is bounded by this server-side window (default 2 minutes). */
  duplicateWindowMs?: number;
  requestTimeoutMs?: number;
}

export type PendingMeshEvent = Omit<MeshEvent, "sequence">;
export interface JetStreamReaderOptions {
  cursorId: string;
  /** Initial sequence only. An existing durable cursor resumes its server checkpoint. */
  after?: number;
  topic?: string;
  to?: string;
}
export interface JetStreamReader {
  /** Server-side blocking pull, not an events.jsonl polling loop. Minimum wait 1000 ms. */
  next(waitMs?: number): Promise<MeshEvent | undefined>;
  /** Confirm a checkpoint only after the caller has durably handled the event. */
  ack(event: MeshEvent): Promise<void>;
  checkpoint(): Promise<number>;
  /** Close leaves the durable checkpoint; deleting a cursor is a separate explicit operation. */
  close(): Promise<void>;
}

export class JetStreamEventLog implements MeshEventLogBackend {
  readonly streamName: string;
  readonly subjectPrefix: string;
  readonly #nc: NatsConnection;
  readonly #js: JetStreamClient;
  readonly #manager: JetStreamManager;
  readonly #maxEventBytes: number;
  readonly #maxReadEvents: number;
  readonly #readers = new Set<JetStreamReader>();
  readonly #readerIds = new Set<string>();
  #closed = false;

  private constructor(options: JetStreamEventLogOptions, nc: NatsConnection, manager: JetStreamManager) {
    const root = options.rootId ?? path.resolve(options.root);
    if (!root) throw new Error("JetStream root identity must not be empty");
    const token = hash(root);
    this.streamName = `FABRIC_${token}`;
    this.subjectPrefix = `fabric.${token}`;
    this.#nc = nc;
    this.#manager = manager;
    this.#js = nc.jetstream({ timeout: options.requestTimeoutMs ?? 5000 });
    this.#maxEventBytes = options.maxEventBytes ?? 64 * 1024;
    this.#maxReadEvents = options.maxReadEvents ?? 1000;
  }

  static async open(options: JetStreamEventLogOptions): Promise<JetStreamEventLog> {
    const maxBytes = options.maxBytes ?? (options.retention === "archive" ? -1 : 64 * 1024 * 1024);
    for (const [key, value] of Object.entries({ maxEventBytes: options.maxEventBytes ?? 64 * 1024,
      maxReadEvents: options.maxReadEvents ?? 1000, duplicateWindowMs: options.duplicateWindowMs ?? 120_000,
      requestTimeoutMs: options.requestTimeoutMs ?? 5000 })) {
      if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`Invalid ${key}`);
    }
    if (!Number.isSafeInteger(maxBytes) || (maxBytes !== -1 && maxBytes <= 0)) throw new Error("Invalid maxBytes");
    if (!Number.isSafeInteger(options.maxAgeMs ?? 0) || (options.maxAgeMs ?? 0) < 0) throw new Error("Invalid maxAgeMs");
    const nc = await connect({ servers: options.servers, timeout: options.requestTimeoutMs ?? 5000,
      reconnect: true, maxReconnectAttempts: -1, reconnectTimeWait: 100, name: "fabric-event-log" });
    try {
      const manager = await nc.jetstreamManager({ timeout: options.requestTimeoutMs ?? 5000 });
      const log = new JetStreamEventLog(options, nc, manager);
      const config: Partial<StreamConfig> = {
        name: log.streamName, subjects: [`${log.subjectPrefix}.*`], storage: StorageType.File,
        retention: RetentionPolicy.Limits, discard: DiscardPolicy.Old, num_replicas: options.replicas ?? 1,
        max_bytes: maxBytes, max_age: nanos(options.maxAgeMs ?? 0), max_msgs: -1,
        max_msgs_per_subject: -1, duplicate_window: nanos(options.duplicateWindowMs ?? 120_000),
        max_msg_size: log.#maxEventBytes, allow_direct: true, no_ack: false,
      };
      let info;
      try { info = await manager.streams.info(log.streamName); }
      catch (error) {
        if (apiCode(error) !== 10059) throw error;
        try { info = await manager.streams.add(config); }
        catch (createError) {
          // Concurrent first-open: read the winner, validate it, never mutate its policy.
          if (apiCode(createError) !== 10058) throw createError;
          info = await manager.streams.info(log.streamName);
        }
      }
      for (const key of ["storage", "retention", "discard", "num_replicas", "max_bytes", "max_age", "duplicate_window",
        "max_msg_size", "max_msgs", "max_msgs_per_subject"] as const) {
        if (info.config[key] !== config[key]) throw new Error(`JetStream stream configuration mismatch: ${key}`);
      }
      if (info.config.subjects?.join("\n") !== config.subjects!.join("\n") || info.config.no_ack) {
        throw new Error("JetStream stream subject/ACK configuration mismatch");
      }
      return log;
    } catch (error) { await nc.close(); throw error; }
  }

  /** Hex encodes the exact UTF-8 topic into one token; dots, slashes and colons cannot collide. */
  subject(topic: string): string {
    topicCheck(topic);
    return `${this.subjectPrefix}.${Buffer.from(topic, "utf8").toString("hex")}`;
  }

  #assertOpen(): void { if (this.#closed) throw new Error("JetStream event log is closed"); }
  #limit(limit = 100): number {
    if (!Number.isFinite(limit)) throw new Error("Invalid mesh read limit");
    return Math.max(1, Math.min(Math.floor(limit), this.#maxReadEvents));
  }

  /** Capture before the network operation; persist this object for retry across process death. */
  prepare(input: MeshPublishInput & { eventId?: string }): PendingMeshEvent {
    this.#assertOpen();
    input.signal?.throwIfAborted();
    topicCheck(input.topic);
    // A synchronous state fence cannot cover a remote asynchronous commit. Reject, never ignore it.
    if (input.fence) throw new Error("JetStream event log does not support synchronous publication fences");
    const createdAt = Date.now();
    const data = typeof input.data === "function" ? (input.data as (time: number) => unknown)(createdAt) : input.data;
    const event: PendingMeshEvent = {
      id: input.eventId ?? (input.dedupeKey ? `key-${hash(`${this.subjectPrefix}\0${input.dedupeKey}`)}` : randomUUID()),
      ...(input.dedupeKey ? { dedupeKey: input.dedupeKey } : {}), topic: input.topic,
      kind: input.kind?.trim() || "message", from: clone(input.from),
      ...(input.to ? { to: input.to } : {}), ...(input.text !== undefined ? { text: input.text } : {}),
      ...(data !== undefined ? { data: clone(data) } : {}), createdAt,
      ...(input.from.verified === "bridge" ? { verification: "bridge" as const }
        : data && typeof data === "object" && "bridge" in data ? {} : { verification: "mesh" as const }),
    };
    const principal = copyFabricPrincipal(input.principal);
    if (principal) event.principal = principal;
    this.#encode(event);
    return event;
  }

  #encode(event: PendingMeshEvent): Uint8Array {
    topicCheck(event.topic);
    // Header decoding trims values; reject ids that would differ from Nats-Msg-Id on the wire.
    if (typeof event.id !== "string" || !event.id || event.id !== event.id.trim() || /[\u0000-\u001f\u007f]/.test(event.id)) {
      throw new Error("Invalid Fabric event id");
    }
    // sequence is deliberately absent in storage: only the stream's committed sequence is authoritative.
    const bytes = Buffer.from(JSON.stringify(event));
    if (bytes.length + 32 > this.#maxEventBytes) throw new Error(`Mesh event exceeds ${this.#maxEventBytes} bytes`);
    return bytes;
  }

  async publishPrepared(pending: PendingMeshEvent): Promise<MeshEvent> {
    this.#assertOpen();
    pending = clone(pending);
    const bytes = this.#encode(pending);
    let ack;
    try {
      ack = await this.#js.publish(this.subject(pending.topic), bytes,
        { msgID: pending.id, expect: { streamName: this.streamName } });
    } catch (error) { throw new JetStreamPublishUncertainError(pending, error); }
    if (ack.duplicate) {
      // A retry can carry changed data/time. The original committed event is authoritative.
      const original = await this.#manager.streams.getMessage(this.streamName, { seq: ack.seq });
      return this.#decode(original.data, ack.seq, original.subject);
    }
    return { ...pending, sequence: ack.seq };
  }

  async publish(input: MeshPublishInput & { eventId?: string }): Promise<MeshEvent> {
    return this.publishPrepared(this.prepare(input));
  }

  /** Ordered, non-atomic prefix. On uncertainty the error carries the exact suffix event to retry. */
  async publishBatch(inputs: MeshPublishInput[]): Promise<MeshEvent[]> {
    if (!inputs.length || inputs.length > 256) throw new Error("Mesh publish batch must contain 1..256 events");
    // Validate and freeze the entire batch before any append.
    const pending = inputs.map(input => this.prepare(input));
    const committed: MeshEvent[] = [];
    for (let i = 0; i < pending.length; i++) {
      try { inputs[i]!.signal?.throwIfAborted(); committed.push(await this.publishPrepared(pending[i]!)); }
      catch (error) { throw new JetStreamBatchPublishError(committed, pending.slice(i), error); }
    }
    return committed;
  }

  #decode(bytes: Uint8Array, sequence: number, subject: string): MeshEvent {
    const event = JSON.parse(Buffer.from(bytes).toString("utf8")) as PendingMeshEvent;
    if (!event.id || this.subject(event.topic) !== subject) throw new Error("Invalid JetStream Fabric event envelope");
    return { ...event, sequence };
  }

  async #boundary(after?: number) {
    this.#assertOpen();
    if (after !== undefined) cursorCheck(after);
    const info = await this.#manager.streams.info(this.streamName);
    if (after !== undefined && after > 0 && after < info.state.last_seq &&
      (!info.state.messages || after + 1 < info.state.first_seq)) {
      throw new JetStreamCursorExpiredError(after, info.state.messages ? info.state.first_seq : info.state.last_seq + 1);
    }
    return info;
  }

  async read(input: EventLogReadOptions = {}): Promise<MeshEvent[]> {
    const limit = this.#limit(input.limit);
    if (input.topic !== undefined) topicCheck(input.topic);
    const boundary = await this.#boundary(input.after);
    const high = boundary.state.last_seq;
    if (!boundary.state.messages || (input.after ?? 0) >= high) return [];
    // Create explicitly: the nats 2.x ordered helper retries initial consumer creation for
    // minutes, even when creation is permanently refused. A one-shot read must fail boundedly.
    // AckNone pull delivery is ordered on the connection; reconnect failure is surfaced for
    // retry from the caller's stream-sequence checkpoint, not hidden behind a polling loop.
    const info = await this.#manager.consumers.add(this.streamName, {
      name: `snapshot_${randomUUID().replaceAll("-", "")}`,
      deliver_policy: DeliverPolicy.StartSequence, opt_start_seq: (input.after ?? 0) + 1,
      filter_subject: input.topic === undefined ? `${this.subjectPrefix}.*` : this.subject(input.topic),
      ack_policy: AckPolicy.None, replay_policy: ReplayPolicy.Instant,
      inactive_threshold: nanos(60_000), num_replicas: 1, mem_storage: true,
    });
    const consumer = this.#js.consumers.getPullConsumerFor(info);
    let messages: ConsumerMessages | undefined;
    try {
      const pending = (await consumer.info()).num_pending;
      if (!pending) return [];
      const events: MeshEvent[] = [];
      let seen = 0, previous = 0, complete = false;
      let resolve!: () => void;
      let reject!: (error: unknown) => void;
      const finished = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
      // Use public consume callback mode. In nats 2.x, breaking the raw pull iterator
      // can queue its own stop behind an iterator that has already returned, hanging close().
      messages = await consumer.consume({ max_messages: 256, expires: 30_000, abort_on_missing_resource: true,
        callback: msg => {
          if (complete) return;
          try {
            const seq = msg.info.streamSequence;
            if (seq <= previous) throw new Error("JetStream snapshot lost stream ordering; retry from checkpoint");
            previous = seq;
            if (seq > high) { complete = true; resolve(); return; }
            const event = this.#decode(msg.data, seq, msg.subject);
            if (input.to === undefined || event.to === input.to) {
              events.push(event);
              if (input.after === undefined && events.length > limit) events.shift();
            }
            if (++seen >= pending || seq >= high || (input.after !== undefined && events.length >= limit)) {
              complete = true; resolve();
            }
          } catch (error) { complete = true; reject(error); }
        },
      });
      const timer = setTimeout(() => { complete = true; reject(new Error("JetStream snapshot read timed out; retry from checkpoint")); }, 30_000);
      void messages.closed().then(error => {
        if (!complete) { complete = true; reject(error ?? new Error("JetStream snapshot closed before its boundary")); }
      });
      try { await finished; return events; } finally { clearTimeout(timer); }
    } finally { await messages?.close(); await consumer.delete(); }
  }

  async tail(cursor: number, limit = 100): Promise<MeshTailResult> {
    const events = await this.read({ after: cursor, limit });
    return { events, nextOffset: events.at(-1)?.sequence ?? cursor, cursors: events.map(event => event.sequence) };
  }
  async nextEventAfter(after: number): Promise<MeshEvent | undefined> { return (await this.read({ after, limit: 1 }))[0]; }
  async latestSequence(): Promise<number> { return (await this.#boundary()).state.last_seq; }
  async oldestSequence(): Promise<number | undefined> {
    const info = await this.#boundary();
    return info.state.messages ? info.state.first_seq : undefined;
  }
  async latestCursor(): Promise<{ cursor: number; last?: { sequence: number; id: string } }> {
    const info = await this.#boundary();
    if (!info.state.messages) return info.state.last_seq === 0
      ? { cursor: 0, last: { sequence: 0, id: "" } } : { cursor: info.state.last_seq };
    const msg = await this.#manager.streams.getMessage(this.streamName, { seq: info.state.last_seq });
    const event = this.#decode(msg.data, msg.seq, msg.subject);
    return { cursor: event.sequence, last: { sequence: event.sequence, id: event.id } };
  }

  async openReader(options: JetStreamReaderOptions): Promise<JetStreamReader> {
    this.#assertOpen();
    if (!options.cursorId) throw new Error("A durable reader requires cursorId");
    if (options.topic !== undefined) topicCheck(options.topic);
    cursorCheck(options.after ?? 0);
    const durable = `cursor_${hash(options.cursorId)}`;
    const filter = options.topic === undefined ? `${this.subjectPrefix}.*` : this.subject(options.topic);
    const config = { durable_name: durable, ack_policy: AckPolicy.Explicit, deliver_policy: DeliverPolicy.StartSequence,
      opt_start_seq: (options.after ?? 0) + 1, filter_subject: filter, replay_policy: ReplayPolicy.Instant,
      max_ack_pending: 1, ack_wait: nanos(1000), num_replicas: 0,
      description: JSON.stringify({ fabricCursor: options.cursorId, to: options.to ?? null }),
    };
    let info;
    try { info = await this.#manager.consumers.info(this.streamName, durable); }
    catch (error) {
      if (apiCode(error) !== 10014) throw error;
      await this.#boundary(options.after);
      try { info = await this.#manager.consumers.add(this.streamName, config); }
      catch (createError) {
        // A racing opener must still bind to exactly this cursor's configuration.
        info = await this.#manager.consumers.info(this.streamName, durable).catch(() => { throw createError; });
      }
    }
    if (info.config.filter_subject !== filter || info.config.description !== config.description ||
      info.config.ack_policy !== AckPolicy.Explicit || info.config.max_ack_pending !== 1) {
      throw new Error("JetStream durable cursor configuration mismatch");
    }
    const consumer: Consumer = await this.#js.consumers.get(this.streamName, durable);
    if (this.#readerIds.has(durable)) throw new Error("A durable cursor already has an active reader in this backend");
    this.#readerIds.add(durable);
    let closed = false;
    let pulling = false;
    let outstanding: { msg: JsMsg; event: MeshEvent } | undefined;
    const reader: JetStreamReader = {
      next: async (waitMs = 30_000) => {
        if (closed || this.#closed) throw new Error("JetStream reader is closed");
        if (pulling || outstanding) throw new Error("A cursor must checkpoint its event before next()");
        if (!Number.isFinite(waitMs) || waitMs < 1000) throw new Error("JetStream pull wait must be >= 1000 ms");
        pulling = true;
        const deadline = Date.now() + waitMs;
        try {
          const cursorInfo = await consumer.info();
          await this.#boundary(cursorInfo.ack_floor.stream_seq || (cursorInfo.config.opt_start_seq ?? 1) - 1);
          for (;;) {
            const msg = await consumer.next({ expires: Math.max(1000, deadline - Date.now()) });
            if (!msg) return undefined;
            const event = this.#decode(msg.data, msg.info.streamSequence, msg.subject);
            if (options.to !== undefined && event.to !== options.to) {
              if (!await msg.ackAck()) throw new Error("JetStream filtered cursor checkpoint not confirmed");
              if (Date.now() >= deadline) return undefined;
              continue;
            }
            outstanding = { msg, event };
            return clone(event);
          }
        } finally { pulling = false; }
      },
      ack: async event => {
        if (closed || this.#closed) throw new Error("JetStream reader is closed");
        if (!outstanding || outstanding.event.id !== event.id || outstanding.event.sequence !== event.sequence) {
          throw new Error("Cannot checkpoint an event not delivered by this reader");
        }
        if (!await outstanding.msg.ackAck()) throw new Error("JetStream cursor checkpoint not confirmed; close/reopen to reconcile");
        outstanding = undefined;
      },
      checkpoint: async () => (await consumer.info()).ack_floor.stream_seq,
      close: async () => {
        if (pulling) throw new Error("Wait for next() before closing its reader");
        closed = true;
        // No ack: an unhandled event must be redelivered after reopen, not lost.
        outstanding = undefined;
        this.#readers.delete(reader);
        this.#readerIds.delete(durable);
      },
    };
    this.#readers.add(reader);
    return reader;
  }

  async deleteReader(cursorId: string): Promise<boolean> {
    this.#assertOpen();
    const durable = `cursor_${hash(cursorId)}`;
    if (this.#readerIds.has(durable)) throw new Error("Close the active reader before deleting its cursor");
    return this.#manager.consumers.delete(this.streamName, durable);
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    // Reject concurrent closes instead of hiding an in-flight caller operation.
    for (const reader of this.#readers) await reader.close();
    this.#closed = true;
    await this.#nc.close();
  }
}
