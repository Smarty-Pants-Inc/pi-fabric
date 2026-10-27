import type { Msg, NatsConnection, Subscription } from "@nats-io/nats-core";
import { SESSION_CHANNEL_OPERATIONS } from "../../mesh/nats-auth/permissions.js";
import { hashToken } from "./subjects.js";

export const SESSION_CHANNEL_REQUEST_LIMIT = 256 * 1024;
export const SESSION_CHANNEL_REPLY_LIMIT = 1024 * 1024;
export const SESSION_CHANNEL_DEDUPE_TTL_MS = 10 * 60_000;
export const SESSION_CHANNEL_DEDUPE_LIMIT = 1_000;

const SERVICE = /^[a-z0-9][a-z0-9-]{0,47}$/;
const ORG = /^[a-z0-9][a-z0-9-]{0,62}$/;
const OPERATION = /^[a-z][a-z0-9-]{0,31}$/;
const REQUEST_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const utf8 = new TextEncoder();
const utf8Fatal = new TextDecoder("utf-8", { fatal: true });
const allowedOperations = new Set<string>(SESSION_CHANNEL_OPERATIONS);

export interface CallerIdentity {
  readonly id: string;
}

export interface SessionChannelRequest {
  readonly requestId: string;
  readonly body: unknown;
  readonly caller: CallerIdentity;
}

export interface SessionChannelReply {
  readonly status: number;
  readonly body: unknown;
}

export type SessionChannelEvent = unknown;

export interface SessionChannelOpenOptions {
  readonly service: string;
  readonly version: number;
}

export interface SessionChannel {
  handle(op: string, handler: (req: SessionChannelRequest) => Promise<SessionChannelReply>): void;
  publish(event: SessionChannelEvent): void;
  close(): void;
}

export interface SessionChannelIdentity {
  readonly org: string;
  readonly hostId: string;
  readonly sessionId: string;
  readonly sessionGeneration: string;
}

export interface SessionChannelSubjects extends SessionChannelIdentity {
  readonly service: string;
}

export const sessionChannelRequestSubject = (input: SessionChannelSubjects, op: string): string =>
  `fabric.svc.${input.org}.${hashToken(input.hostId)}.${hashToken(input.sessionId)}.${input.service}.${op}`;

export const sessionChannelEventsSubject = (input: SessionChannelSubjects): string =>
  `fabric.svc.${input.org}.${hashToken(input.hostId)}.${hashToken(input.sessionId)}.${input.service}.events`;

interface DedupeEntry {
  expiresAt: number;
  response?: Uint8Array;
  pending?: Promise<Uint8Array>;
}

interface ChannelRecord {
  readonly service: string;
  readonly version: number;
  readonly handlers: Map<string, (req: SessionChannelRequest) => Promise<SessionChannelReply>>;
  readonly identity: SessionChannelIdentity;
  subscription?: Subscription;
  closed: boolean;
}

const matches = (pattern: RegExp, value: string): boolean => pattern.exec(value)?.[0] === value;

const plainObject = (value: unknown): value is Record<string, unknown> => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
};

const replyBytes = (status: number, body: unknown): Uint8Array => {
  const encoded = utf8.encode(JSON.stringify({ status, body }));
  if (encoded.byteLength > SESSION_CHANNEL_REPLY_LIMIT) throw new Error("session channel reply exceeds limit");
  return encoded;
};

export class NatsSessionChannelManager {
  readonly #channels = new Map<string, ChannelRecord>();
  readonly #dedupe = new Map<string, DedupeEntry>();
  #connection: NatsConnection | undefined;
  #identity: SessionChannelIdentity | undefined;
  #closed = false;

  constructor(connection?: NatsConnection) {
    this.#connection = connection;
  }

  async open(identity: SessionChannelIdentity, options: SessionChannelOpenOptions): Promise<SessionChannel> {
    if (this.#closed) throw new Error("session channel transport is closed");
    this.#validateIdentity(identity);
    if (this.#identity && (this.#identity.org !== identity.org || this.#identity.hostId !== identity.hostId ||
      this.#identity.sessionId !== identity.sessionId || this.#identity.sessionGeneration !== identity.sessionGeneration)) {
      throw new Error("session channel identity changed within one transport");
    }
    this.#identity ??= identity;
    if (!matches(SERVICE, options.service)) throw new Error("session channel service must be one lowercase NATS token");
    if (!Number.isSafeInteger(options.version) || options.version < 1) throw new Error("session channel version must be a positive integer");
    if (this.#channels.has(options.service)) throw new Error("a session channel is already open for this service");
    const record: ChannelRecord = {
      service: options.service,
      version: options.version,
      handlers: new Map(),
      identity,
      closed: false,
    };
    this.#channels.set(record.service, record);
    try {
      this.#bind(record);
    } catch (cause) {
      this.#closeChannel(record);
      throw cause;
    }
    const connection = this.#connection;
    if (!connection || connection.isClosed() || connection.isDraining()) {
      this.#closeChannel(record);
      throw new Error("session channel transport is unavailable");
    }
    try {
      await connection.flush();
    } catch (cause) {
      this.#closeChannel(record);
      throw new Error("session channel subscription could not be installed", { cause });
    }
    if (record.closed || this.#connection !== connection || connection.isClosed() || connection.isDraining()) {
      this.#closeChannel(record);
      throw new Error("session channel transport changed while opening");
    }
    return {
      handle: (op, handler) => {
        if (record.closed) throw new Error("session channel is closed");
        if (!matches(OPERATION, op) || op === "events" || !allowedOperations.has(op)) throw new Error("unsupported session channel operation");
        if (typeof handler !== "function") throw new Error("session channel handler must be a function");
        record.handlers.set(op, handler);
      },
      publish: (event) => {
        if (record.closed) throw new Error("session channel is closed");
        const connection = this.#connection;
        if (!connection || connection.isClosed() || connection.isDraining()) throw new Error("session channel transport is unavailable");
        const json = JSON.stringify(event);
        if (typeof json !== "string") throw new Error("session channel event is not JSON-serializable");
        connection.publish(sessionChannelEventsSubject({ ...record.identity, service: record.service }), utf8.encode(json));
      },
      close: () => this.#closeChannel(record),
    };
  }

  setConnection(connection: NatsConnection | undefined): void {
    if (this.#closed || this.#connection === connection) return;
    for (const record of this.#channels.values()) {
      record.subscription?.unsubscribe();
      delete record.subscription;
    }
    this.#connection = connection;
    if (connection) for (const record of this.#channels.values()) this.#bind(record);
  }

  services(): Array<{ name: string; version: number }> {
    return [...this.#channels.values()]
      .filter((record) => !record.closed)
      .map(({ service, version }) => ({ name: service, version }))
      .sort((a, b) => a.name.localeCompare(b.name));
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const record of [...this.#channels.values()]) this.#closeChannel(record);
    this.#connection = undefined;
    this.#dedupe.clear();
  }

  #validateIdentity(identity: SessionChannelIdentity): void {
    if (typeof identity.org !== "string" || !matches(ORG, identity.org)) throw new Error("session channel org is required");
    if (typeof identity.hostId !== "string" || identity.hostId.length === 0) throw new Error("session channel hostId is required");
    if (typeof identity.sessionId !== "string" || !matches(REQUEST_ID, identity.sessionId)) throw new Error("session channel sessionId must be a UUID");
    if (typeof identity.sessionGeneration !== "string" || identity.sessionGeneration.length === 0) throw new Error("session channel sessionGeneration is required");
  }

  #isVerified(connection = this.#connection): boolean {
    if (!connection || this.#connection !== connection) return false;
    const check = (connection as NatsConnection & { isVerified?: () => boolean }).isVerified;
    return typeof check !== "function" || check.call(connection);
  }

  #bind(record: ChannelRecord): void {
    const connection = this.#connection;
    if (!connection || this.#closed || record.closed) return;
    const subject = sessionChannelRequestSubject({ ...record.identity, service: record.service }, "*");
    const subscription = connection.subscribe(subject, {
      callback: (_error, message) => {
        if (this.#connection !== connection || !this.#isVerified()) return;
        void this.#receive(record, message).catch(() => undefined);
      },
    });
    record.subscription = subscription;
  }

  #closeChannel(record: ChannelRecord): void {
    if (record.closed) return;
    record.closed = true;
    record.subscription?.unsubscribe();
    delete record.subscription;
    record.handlers.clear();
    this.#channels.delete(record.service);
  }

  #pruneDedupe(now: number): void {
    for (const [id, entry] of this.#dedupe) {
      if (entry.response && entry.expiresAt <= now) this.#dedupe.delete(id);
    }
  }

  #makeDedupeRoom(): boolean {
    while (this.#dedupe.size >= SESSION_CHANNEL_DEDUPE_LIMIT) {
      const oldestCompleted = [...this.#dedupe].find(([, entry]) => entry.response !== undefined);
      if (!oldestCompleted) return false;
      this.#dedupe.delete(oldestCompleted[0]);
    }
    return true;
  }

  #touchDedupe(id: string, entry: DedupeEntry): void {
    this.#dedupe.delete(id);
    this.#dedupe.set(id, entry);
  }

  async #receive(record: ChannelRecord, message: Msg): Promise<void> {
    const connection = this.#connection;
    if (!connection || record.closed || !message.reply || !this.#isVerified(connection)) return;
    const caller = this.#readCaller(message);
    if (!caller) {
      this.#respond(message, 400, { error: "invalid request" }, connection);
      return;
    }
    const requestId = message.headers?.get("Fabric-Request-Id") ?? "";
    if (!matches(REQUEST_ID, requestId)) {
      this.#respond(message, 400, { error: "missing request id" }, connection);
      return;
    }
    if (message.data.byteLength > SESSION_CHANNEL_REQUEST_LIMIT) {
      this.#respond(message, 413, { error: "request too large" }, connection);
      return;
    }

    const op = message.subject.split(".").at(-1) ?? "";
    const dedupeKey = JSON.stringify([caller.id, op, requestId]);
    this.#pruneDedupe(Date.now());
    const prior = this.#dedupe.get(dedupeKey);
    if (prior) {
      this.#touchDedupe(dedupeKey, prior);
      const bytes = prior.response ?? await prior.pending;
      if (bytes) this.#respondMessage(message, bytes, connection);
      return;
    }
    if (!this.#makeDedupeRoom()) return; // Bounded in-flight custody: no reply means caller outcome is unknown.
    const entry: DedupeEntry = { expiresAt: Number.POSITIVE_INFINITY };
    this.#dedupe.set(dedupeKey, entry);
    entry.pending = this.#dispatch(record, message, requestId, caller).then((bytes) => {
      entry.response = bytes;
      delete entry.pending;
      entry.expiresAt = Date.now() + SESSION_CHANNEL_DEDUPE_TTL_MS;
      return bytes;
    }).catch(() => {
      const bytes = replyBytes(500, { error: "handler failed" });
      entry.response = bytes;
      delete entry.pending;
      entry.expiresAt = Date.now() + SESSION_CHANNEL_DEDUPE_TTL_MS;
      return bytes;
    });
    this.#touchDedupe(dedupeKey, entry);
    const bytes = await entry.pending;
    if (bytes) this.#respondMessage(message, bytes, connection);
  }

  #readCaller(message: Msg): CallerIdentity | undefined {
    const value = message.headers?.get("Fabric-Caller");
    if (!value) return undefined;
    try {
      const parsed: unknown = JSON.parse(value);
      if (!plainObject(parsed) || Object.keys(parsed).length !== 1 || typeof parsed.id !== "string" ||
        parsed.id.length === 0 || utf8.encode(parsed.id).byteLength > 256) return undefined;
      return { id: parsed.id };
    } catch {
      return undefined;
    }
  }

  async #dispatch(record: ChannelRecord, message: Msg, requestId: string, caller: CallerIdentity): Promise<Uint8Array> {
    let body: unknown;
    try {
      body = JSON.parse(utf8Fatal.decode(message.data));
    } catch {
      return replyBytes(400, { error: "invalid request" });
    }
    const op = message.subject.split(".").at(-1) ?? "";
    const handler = record.handlers.get(op);
    if (!handler) return replyBytes(404, { error: "unknown operation" });
    try {
      const result = await handler({ requestId, body, caller });
      if (!result || !Number.isSafeInteger(result.status) || result.status < 100 || result.status > 599) {
        throw new Error("invalid handler reply");
      }
      return replyBytes(result.status, result.body);
    } catch {
      return replyBytes(500, { error: "handler failed" });
    }
  }

  #respond(message: Msg, status: number, body: unknown, connection: NatsConnection): void {
    this.#respondMessage(message, replyBytes(status, body), connection);
  }

  #respondMessage(message: Msg, bytes: Uint8Array, connection: NatsConnection): void {
    if (!this.#isVerified(connection)) return;
    try { message.respond(bytes); } catch { /* No raw handler or serialization error reaches the wire. */ }
  }
}
