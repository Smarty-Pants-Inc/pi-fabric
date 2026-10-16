import { Buffer } from "node:buffer";
import {
  type ConsumerMessages,
  type JsMsg,
  type JetStreamClient,
  type JetStreamManager,
} from "@nats-io/jetstream";
import type {
  FabricTransport,
  FabricTransportOptions,
  TransportDelivery,
  TransportEnvelope,
  TransportOwner,
  TransportSent,
} from "../types.js";
import { TransportInvalidError, TransportUncertainError, TransportUnavailableError } from "../types.js";
import { publishFailure } from "./errors.js";
import {
  transportAdvisoryStreamSubject,
  transportDurableName,
  transportInboxFilter,
  transportInboxStream,
  transportInboxStreamSubject,
  transportInboxSubject,
  validateMachine,
  hashToken,
} from "./subjects.js";
import { ensureTransportConsumer, ensureTransportStream, type TransportStreamPolicy } from "./provision.js";
import { ADVISORY_CAPTURE_STREAM } from "./subjects.js";
import { openNatsConnection, type NatsConnectionHandle } from "./connection.js";
import { openNatsKvForMachine, type NatsKvStore } from "./kv.js";

const DEFAULT_MAX_BYTES = 1_073_741_824;
const DEFAULT_MAX_MESSAGE_BYTES = 1_048_576;
const DEFAULT_DUPLICATE_WINDOW_MS = 10 * 60 * 1_000;
const DEFAULT_REQUEST_TIMEOUT_MS = 5_000;
const utf8 = new TextEncoder();
const clone = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;

/** Direct options keep the module independently testable; the factory passes FabricTransportOptions. */
export interface NatsTransportOptions {
  readonly servers: string | readonly string[];
  readonly machine: string;
  readonly hostId: string;
  readonly name?: string;
  readonly credentials?: string;
  readonly maxBytes?: number;
  readonly maxAgeMs?: number;
  readonly maxMessageBytes?: number;
  readonly duplicateWindowMs?: number;
  readonly requestTimeoutMs?: number;
  readonly provision?: boolean;
  readonly verifyMachine?: boolean;
}

type OpenOptions = NatsTransportOptions | FabricTransportOptions;
type ResolvedNatsOptions = Required<Pick<NatsTransportOptions, "servers" | "machine" | "hostId" | "maxBytes" | "maxMessageBytes" | "duplicateWindowMs" | "requestTimeoutMs" | "provision" | "verifyMachine">> & {
  readonly name?: string;
  readonly credentials?: string;
};

const isDirectOptions = (options: OpenOptions): options is NatsTransportOptions => "servers" in options;

const envString = (name: string): string | undefined => {
  const value = process.env[name]?.trim();
  return value ? value : undefined;
};

const normalizeOptions = (options: OpenOptions): ResolvedNatsOptions => {
  if (isDirectOptions(options)) {
    return {
      servers: options.servers,
      machine: validateMachine(options.machine),
      hostId: options.hostId,
      ...(options.name === undefined ? {} : { name: options.name }),
      ...(options.credentials === undefined ? {} : { credentials: options.credentials }),
      maxBytes: options.maxBytes ?? DEFAULT_MAX_BYTES,
      maxMessageBytes: options.maxMessageBytes ?? DEFAULT_MAX_MESSAGE_BYTES,
      duplicateWindowMs: options.duplicateWindowMs ?? DEFAULT_DUPLICATE_WINDOW_MS,
      requestTimeoutMs: options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
      provision: options.provision ?? options.credentials === undefined,
      verifyMachine: options.verifyMachine ?? true,
    } as ResolvedNatsOptions;
  }
  const config = options.config as unknown as { nats?: { servers?: string | readonly string[]; machine?: string; credentials?: string } };
  const servers = config.nats?.servers ?? envString("PI_FABRIC_NATS_SERVERS");
  const machine = config.nats?.machine ?? envString("PI_FABRIC_MACHINE");
  const credentials = config.nats?.credentials ?? envString("PI_FABRIC_NATS_CREDENTIALS");
  if (!servers || !machine || !credentials) throw new Error("NATS transport requires servers, machine and credentials");
  return {
    servers,
    machine: validateMachine(machine),
    hostId: options.hostId,
    ...(envString("PI_FABRIC_NATS_NAME") === undefined ? {} : { name: envString("PI_FABRIC_NATS_NAME") }),
    credentials,
    maxBytes: DEFAULT_MAX_BYTES,
    maxMessageBytes: DEFAULT_MAX_MESSAGE_BYTES,
    duplicateWindowMs: DEFAULT_DUPLICATE_WINDOW_MS,
    requestTimeoutMs: 5_000,
    provision: false,
    verifyMachine: true,
  } as ResolvedNatsOptions;
};

const streamPolicy = (name: string, subjects: readonly string[], options: ResolvedNatsOptions, overrides: Partial<TransportStreamPolicy> = {}): TransportStreamPolicy => ({
  name,
  subjects,
  maxBytes: overrides.maxBytes ?? options.maxBytes,
  maxAgeMs: overrides.maxAgeMs ?? 0,
  maxMessageBytes: overrides.maxMessageBytes ?? options.maxMessageBytes,
  duplicateWindowMs: overrides.duplicateWindowMs ?? options.duplicateWindowMs,
  replicas: 1,
});

const isTransportEnvelope = (value: unknown): value is TransportEnvelope => {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const candidate = value as Record<string, unknown>;
  const to = candidate.to;
  return typeof candidate.id === "string" && candidate.id.length > 0 &&
    (candidate.kind === "command" || candidate.kind === "ack") &&
    typeof candidate.targetId === "string" && candidate.targetId.length > 0 &&
    typeof candidate.operation === "string" && candidate.operation.length > 0 &&
    typeof candidate.from === "object" && candidate.from !== null &&
    typeof to === "object" && to !== null &&
    typeof (to as Record<string, unknown>).machine === "string" &&
    typeof (to as Record<string, unknown>).hostId === "string";
};

class NatsTransportImpl implements FabricTransport {
  readonly kind = "nats" as const;
  readonly self: TransportOwner;
  readonly #connection: NatsConnectionHandle;
  readonly #js: JetStreamClient;
  readonly #manager: JetStreamManager;
  readonly #stream: string;
  readonly #options: ResolvedNatsOptions;
  readonly #kvStores: Readonly<Record<"leases" | "presence", NatsKvStore>>;
  readonly #closeController = new AbortController();
  #closed = false;
  #inboxActive = false;
  readonly #inboxMessages = new Set<ConsumerMessages>();

  private constructor(connection: NatsConnectionHandle, options: ResolvedNatsOptions, kvStores: Readonly<Record<"leases" | "presence", NatsKvStore>>) {
    this.#connection = connection;
    this.#js = connection.js;
    this.#manager = connection.manager;
    this.#stream = transportInboxStream(options.machine);
    this.#options = options;
    this.#kvStores = kvStores;
    this.self = { machine: options.machine, hostId: options.hostId };
  }

  static async open(rawOptions: OpenOptions): Promise<NatsTransportImpl> {
    const options = normalizeOptions(rawOptions);
    if (!options.hostId) throw new Error("hostId is required");
    for (const [key, value] of Object.entries(options)) {
      if (["servers", "machine", "hostId", "name", "credentials"].includes(key)) continue;
      if (typeof value === "number" && (!Number.isSafeInteger(value) || value <= 0)) throw new Error(`${key} must be positive`);
    }
    const connection = await openNatsConnection({
      servers: options.servers,
      ...(options.credentials ? { credentials: options.credentials } : {}),
      ...(options.name ? { name: options.name } : {}),
      machine: options.machine,
      verifyMachine: options.verifyMachine,
      timeoutMs: options.requestTimeoutMs,
    });
    try {
      const { manager } = connection;
      // Provisioning authority creates capture first. Runtime credentials cannot read or
      // administer FABRIC_ADV; they bind only the operator-provisioned inbox and KV buckets.
      if (options.provision) {
        await ensureTransportStream(manager, streamPolicy(ADVISORY_CAPTURE_STREAM, [transportAdvisoryStreamSubject], options, {
          maxAgeMs: 30 * 24 * 60 * 60 * 1_000,
          maxBytes: Math.max(options.maxBytes, 64 * 1024 * 1024),
          maxMessageBytes: 64 * 1024,
        }), true);
      }
      const leases = await openNatsKvForMachine(connection.js, "leases", options.machine, options.provision, connection.onStatus);
      const presence = await openNatsKvForMachine(connection.js, "presence", options.machine, options.provision, connection.onStatus);
      await ensureTransportStream(manager, streamPolicy(transportInboxStream(options.machine), [transportInboxStreamSubject(options.machine)], options, {
        maxAgeMs: 0,
      }), options.provision);
      await ensureTransportConsumer(manager.consumers, {
        stream: transportInboxStream(options.machine),
        durable: transportDurableName(options.hostId),
        filterSubject: transportInboxFilter(options.machine, options.hostId),
        description: JSON.stringify({ fabricHostId: options.hostId, machine: options.machine }),
        maxAckPending: 128,
      });
      return new NatsTransportImpl(connection, options, { leases, presence });
    } catch (error) {
      await connection.close();
      throw error;
    }
  }

  #assertOpen(): void {
    if (this.#closed || this.#connection.nc.isClosed() || this.#connection.nc.isDraining()) {
      throw new TransportUnavailableError("NATS transport is closed");
    }
  }

  #wire<T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): { resolved: TransportSent<T>["envelope"]; subject: string; bytes: Uint8Array } {
    this.#assertOpen();
    signal?.throwIfAborted();
    if (!isTransportEnvelope(envelope)) throw new TransportInvalidError("invalid transport envelope");
    const committedAt = Date.now();
    const body = typeof envelope.body === "function" ? (envelope.body as (at: number) => T)(committedAt) : envelope.body;
    const resolved = { ...clone(envelope), body, createdAt: committedAt } as TransportSent<T>["envelope"];
    const kind = envelope.kind === "command" ? "cmd" : "ack";
    const subject = transportInboxSubject(envelope.to.machine, envelope.to.hostId, kind, envelope.targetId, this.self.machine);
    const bytes = utf8.encode(JSON.stringify({ format: 1, sourceMachine: this.self.machine, envelope: resolved }));
    if (bytes.byteLength > this.#options.maxMessageBytes) throw new TransportInvalidError(`transport envelope exceeds ${this.#options.maxMessageBytes} bytes`);
    return { resolved, subject, bytes };
  }

  async send<T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): Promise<TransportSent<T>> {
    const prepared = this.#wire(envelope, signal);
    let ack;
    try {
      ack = await this.#js.publish(prepared.subject, prepared.bytes, {
        msgID: envelope.id,
        timeout: this.#options.requestTimeoutMs,
        expect: { streamName: this.#streamFor(envelope.to.machine) },
      });
    } catch (error) {
      throw publishFailure(error, envelope.id, prepared.resolved.body);
    }
    if (signal?.aborted) throw new TransportUncertainError(envelope.id, prepared.resolved.body, { cause: signal.reason });
    return { id: envelope.id, sequence: ack.seq, duplicate: ack.duplicate, envelope: prepared.resolved };
  }

  #streamFor(machine: string): string { return transportInboxStream(validateMachine(machine)); }

  #decode(msg: JsMsg): { envelope: TransportEnvelope; createdAt: number; sourceMachine: string } {
    let value: unknown;
    try { value = JSON.parse(Buffer.from(msg.data).toString("utf8")); } catch { throw new Error("invalid transport envelope JSON"); }
    if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error("invalid transport envelope");
    const record = value as Record<string, unknown>;
    const envelope = record.envelope;
    const sourceMachine = record.sourceMachine;
    if (record.format !== 1 || typeof sourceMachine !== "string" || !isTransportEnvelope(envelope) ||
      typeof (envelope as unknown as Record<string, unknown>).createdAt !== "number") throw new Error("invalid transport envelope");
    const parts = msg.subject.split(".");
    if (parts.length !== 7 || parts[0] !== "fabric" || parts[1] !== "in" || parts[2] !== this.self.machine ||
      parts[3] !== hashToken(envelope.to.hostId) || parts[4] !== (envelope.kind === "command" ? "cmd" : "ack") ||
      parts[5] !== hashToken(envelope.targetId) || parts[6] !== sourceMachine) throw new Error("transport subject/payload mismatch");
    if (envelope.to.machine !== this.self.machine || envelope.to.hostId !== this.self.hostId || sourceMachine !== validateMachine(sourceMachine)) throw new Error("transport destination mismatch");
    return { envelope, createdAt: (envelope as TransportEnvelope & { createdAt: number }).createdAt, sourceMachine };
  }

  async *inbox(signal: AbortSignal): AsyncIterable<TransportDelivery<TransportEnvelope>> {
    this.#assertOpen();
    signal.throwIfAborted();
    if (this.#inboxActive) throw new TransportInvalidError("only one inbox iterator is allowed per transport");
    this.#inboxActive = true;
    let messages: ConsumerMessages | undefined;
    const stop = (): void => { if (messages) void messages.close(); };
    signal.addEventListener("abort", stop, { once: true });
    this.#closeController.signal.addEventListener("abort", stop, { once: true });
    try {
    const durable = transportDurableName(this.self.hostId);
    const filter = transportInboxFilter(this.self.machine, this.self.hostId);
    const info = await ensureTransportConsumer(this.#manager.consumers, {
      stream: this.#stream,
      durable,
      filterSubject: filter,
      description: JSON.stringify({ fabricHostId: this.self.hostId, machine: this.self.machine }),
      maxAckPending: 128,
    });
    if (info.config.filter_subject !== filter) throw new Error("transport inbox filter mismatch");
    const consumer = await this.#js.consumers.get(this.#stream, durable);
    messages = await consumer.consume({ max_messages: 16 });
    this.#inboxMessages.add(messages);
    if (signal.aborted || this.#closed) { await messages.close(); return; }
      for await (const msg of messages) {
        if (signal.aborted || this.#closed) break;
        let decoded;
        try { decoded = this.#decode(msg); }
        catch { await msg.term("malformed transport envelope"); continue; }
        const payload = decoded.envelope;
        let finished = false;
        const delivery: TransportDelivery<TransportEnvelope> = {
          payload,
          sourceMachine: decoded.sourceMachine,
          verification: "bridge",
          attempt: msg.info.deliveryCount,
          sequence: msg.info.streamSequence,
          working: () => { if (!finished) msg.working(); },
          ack: async () => {
            if (finished) return;
            const confirmed = await msg.ackAck();
            if (!confirmed) throw new Error("transport broker ACK was not confirmed");
            finished = true;
          },
          nak: (delayMs?: number) => { if (!finished) msg.nak(delayMs); },
          term: async (reason: string) => { if (!finished) { msg.term(reason); finished = true; } },
        };
        yield delivery;
      }
    } finally {
      signal.removeEventListener("abort", stop);
      this.#closeController.signal.removeEventListener("abort", stop);
      if (messages) { this.#inboxMessages.delete(messages); await messages.close(); }
      this.#inboxActive = false;
    }
  }

  kv(bucket: "leases" | "presence"): NatsKvStore {
    this.#assertOpen();
    return this.#kvStores[bucket];
  }

  async drainState(signal?: AbortSignal): Promise<{ pending: number; ackPending: number }> {
    this.#assertOpen();
    signal?.throwIfAborted();
    const info = await this.#manager.consumers.info(this.#stream, transportDurableName(this.self.hostId));
    signal?.throwIfAborted();
    return { pending: info.num_pending, ackPending: info.num_ack_pending };
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#closeController.abort();
    await Promise.all([...this.#inboxMessages].map((messages) => messages.close()));
    await this.#connection.close();
  }
}

export function openNatsTransport(options: FabricTransportOptions): Promise<FabricTransport>;
export function openNatsTransport(options: NatsTransportOptions): Promise<FabricTransport>;
export function openNatsTransport(options: OpenOptions): Promise<FabricTransport> {
  return NatsTransportImpl.open(options);
}
