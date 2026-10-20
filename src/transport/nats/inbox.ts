import { Buffer } from "node:buffer";
import { Readable } from "node:stream";
import { setTimeout as sleep } from "node:timers/promises";
import {
  type ConsumerMessages,
  type JsMsg,
  type JetStreamClient,
  type JetStreamManager,
} from "@nats-io/jetstream";
import type { PlacementTerminalRecord } from "../../placement/terminal.js";
import type {
  FabricTransport,
  FabricTransportOptions,
  TransportDelivery,
  TransportEnvelope,
  TransportKv,
  TransportKvEntry,
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
import { ensureTransportConsumer, ensureTransportStream, type TransportConsumerPolicy, type TransportStreamPolicy } from "./provision.js";
import { ADVISORY_CAPTURE_STREAM } from "./subjects.js";
import { openNatsConnection, type NatsConnectionHandle, type NatsPermissionPolicy } from "./connection.js";
import { NatsSessionChannelManager, type SessionChannel, type SessionChannelIdentity, type SessionChannelOpenOptions } from "./session-channel.js";
import type { TlsOptions } from "@nats-io/transport-node";
import {
  LEASE_TTL_MS, PRESENCE_TTL_MS, PLACEMENT_TERMINAL_BUCKET, PLACEMENT_TERMINAL_TTL_MS,
  openNatsKv, openNatsKvForMachine, type NatsKvStore,
} from "./kv.js";
import type { MeshEventPublisherBinding, MeshStore } from "../../mesh/store.js";
import type { NatsEventsPublisher } from "./events-publish.js";

const DEFAULT_MAX_BYTES = 1_073_741_824;
const DEFAULT_MAX_MESSAGE_BYTES = 1_048_576;
const DEFAULT_DUPLICATE_WINDOW_MS = 10 * 60 * 1_000;
const DEFAULT_REQUEST_TIMEOUT_MS = 5_000;
const DEFAULT_EPHEMERAL_INACTIVE_SECONDS = 3_600;
const EPHEMERAL_DELETE_TIMEOUT_MS = 1_000;
const utf8 = new TextEncoder();
const validMessageId = (id: string): boolean => id.length > 0 && id === id.trim() && !/[\u0000-\u001f\u007f]/.test(id);

/** Direct options keep the module independently testable; the factory passes FabricTransportOptions. */
export interface NatsTransportOptions extends NatsPermissionPolicy {
  readonly servers: string | readonly string[];
  readonly machine: string;
  readonly hostId: string;
  readonly name?: string;
  /** Scoped runtime agent credentials; verified against the exact USER.INFO scope. */
  readonly credentials: string;
  readonly tls?: TlsOptions;
  readonly maxBytes?: number;
  readonly maxAgeMs?: number;
  readonly maxMessageBytes?: number;
  readonly duplicateWindowMs?: number;
  readonly requestTimeoutMs?: number;
  readonly provision?: boolean;
  /** Host-only opt-in to expiring this host's own inbox durable. */
  readonly ephemeralHost?: boolean;
  /** Idle expiration in seconds when ephemeralHost is enabled (300..86400). */
  readonly ephemeralInactiveSeconds?: number;
  /** Opt-in actor-topic publishing, using this transport's verified connection. */
  readonly events?: boolean;
  readonly mesh?: MeshStore;
  readonly signal?: AbortSignal;
}

type OpenOptions = NatsTransportOptions | FabricTransportOptions;
type ResolvedNatsOptions = Required<Pick<NatsTransportOptions, "servers" | "machine" | "hostId" | "credentials" | "custodyStream" | "livenessStream" | "pathBMachines" | "maxBytes" | "maxMessageBytes" | "duplicateWindowMs" | "requestTimeoutMs" | "provision" | "ephemeralHost" | "ephemeralInactiveSeconds" | "events">> & {
  readonly org?: string;
  readonly name?: string;
  readonly tls?: TlsOptions;
  readonly mesh?: MeshStore;
  readonly signal?: AbortSignal;
};

const isDirectOptions = (options: OpenOptions): options is NatsTransportOptions => "servers" in options;

const envString = (name: string): string | undefined => {
  const value = process.env[name]?.trim();
  return value ? value : undefined;
};

const normalizeOptions = (options: OpenOptions): ResolvedNatsOptions => {
  if (isDirectOptions(options)) {
    // The runtime role is fixed to the scoped agent; no caller-selected role or verification bypass.
    for (const key of ["role", "verifyMachine", "allowInsecureLoopback"]) if (key in options) throw new Error(`NATS transport option ${key} is not supported`);
    if (!options.credentials) throw new Error("NATS transport requires credentials");
    return {
      servers: options.servers,
      machine: validateMachine(options.machine),
      hostId: options.hostId,
      ...(options.name === undefined ? {} : { name: options.name }),
      credentials: options.credentials,
      custodyStream: options.custodyStream,
      livenessStream: options.livenessStream,
      pathBMachines: options.pathBMachines,
      ...(options.org === undefined ? {} : { org: options.org }),
      ...(options.tls === undefined ? {} : { tls: options.tls }),
      maxBytes: options.maxBytes ?? DEFAULT_MAX_BYTES,
      maxMessageBytes: options.maxMessageBytes ?? DEFAULT_MAX_MESSAGE_BYTES,
      duplicateWindowMs: options.duplicateWindowMs ?? DEFAULT_DUPLICATE_WINDOW_MS,
      requestTimeoutMs: options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
      // Event-mode runtimes never provision streams, including through the direct-options port.
      provision: options.events === true ? false : options.provision ?? false,
      ephemeralHost: options.ephemeralHost ?? false,
      ephemeralInactiveSeconds: options.ephemeralInactiveSeconds ?? DEFAULT_EPHEMERAL_INACTIVE_SECONDS,
      events: options.events ?? false,
      ...(options.mesh === undefined ? {} : { mesh: options.mesh }),
      ...(options.signal === undefined ? {} : { signal: options.signal }),
    } as ResolvedNatsOptions;
  }
  const config = options.config as unknown as { nats?: {
    servers?: string | readonly string[]; machine?: string; credentials?: string;
    custodyStream?: string; livenessStream?: string; pathBMachines?: readonly string[]; org?: string; tls?: TlsOptions;
    ephemeralHost?: boolean; ephemeralInactiveSeconds?: number; events?: boolean;
  } };
  const nats = config.nats;
  // Production config never enables plaintext; the loopback opt-in is a direct test option only.
  if (nats !== undefined && nats !== null && typeof nats === "object" && "allowInsecureLoopback" in nats) {
    throw new Error("mesh.nats.allowInsecureLoopback is not supported");
  }
  const servers = nats?.servers ?? envString("PI_FABRIC_NATS_SERVERS");
  const machine = nats?.machine ?? envString("PI_FABRIC_MACHINE");
  const credentials = nats?.credentials ?? envString("PI_FABRIC_NATS_CREDENTIALS");
  if (!servers || !machine || !credentials) throw new Error("NATS transport requires servers, machine and credentials");
  // Expected policy is required; openNatsConnection validates it with the mint validators and fails closed.
  return {
    servers,
    machine: validateMachine(machine),
    hostId: options.hostId,
    ...(envString("PI_FABRIC_NATS_NAME") === undefined ? {} : { name: envString("PI_FABRIC_NATS_NAME") }),
    credentials,
    custodyStream: nats?.custodyStream as string,
    livenessStream: nats?.livenessStream as string,
    pathBMachines: nats?.pathBMachines as readonly string[],
    ...(nats?.org === undefined ? {} : { org: nats.org }),
    ...(nats?.tls === undefined ? {} : { tls: nats.tls }),
    maxBytes: DEFAULT_MAX_BYTES,
    maxMessageBytes: DEFAULT_MAX_MESSAGE_BYTES,
    duplicateWindowMs: DEFAULT_DUPLICATE_WINDOW_MS,
    requestTimeoutMs: 5_000,
    provision: false,
    ephemeralHost: nats?.ephemeralHost ?? false,
    ephemeralInactiveSeconds: nats?.ephemeralInactiveSeconds ?? DEFAULT_EPHEMERAL_INACTIVE_SECONDS,
    events: nats?.events ?? false,
    mesh: options.mesh,
    ...(options.signal === undefined ? {} : { signal: options.signal }),
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

const transportConsumerPolicy = (options: ResolvedNatsOptions): TransportConsumerPolicy => ({
  stream: transportInboxStream(options.machine),
  durable: transportDurableName(options.hostId),
  filterSubject: transportInboxFilter(options.machine, options.hostId),
  description: JSON.stringify({ fabricHostId: options.hostId, machine: options.machine }),
  maxAckPending: 128,
  ...(options.ephemeralHost ? { inactiveThresholdNs: options.ephemeralInactiveSeconds * 1_000_000_000 } : {}),
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

interface NatsSession {
  readonly connection: NatsConnectionHandle;
  readonly js: JetStreamClient;
  readonly manager: JetStreamManager;
  readonly kvStores: Readonly<Record<"leases" | "presence", NatsKvStore>>;
  readonly eventsPublisher?: NatsEventsPublisher;
  unsubscribe?: () => void;
}

type ReadyWaiter = {
  readonly resolve: (session: NatsSession) => void;
  readonly reject: (error: unknown) => void;
  readonly signal: AbortSignal;
  onAbort?: () => void;
};

const openNatsSession = async (options: ResolvedNatsOptions, signal?: AbortSignal): Promise<NatsSession> => {
  signal?.throwIfAborted();
  const connection = await openNatsConnection({
    servers: options.servers,
    credentials: options.credentials,
    ...(options.name ? { name: options.name } : {}),
    machine: options.machine,
    custodyStream: options.custodyStream,
    livenessStream: options.livenessStream,
    pathBMachines: options.pathBMachines,
    ...(options.events ? { events: true, org: options.org! } : options.org === undefined ? {} : { org: options.org }),
    ...(options.tls === undefined ? {} : { tls: options.tls }),
    timeoutMs: options.requestTimeoutMs,
    ...(signal === undefined ? {} : { signal }),
  });
  const assertAlive = (): void => signal?.throwIfAborted();
  try {
    assertAlive();
    const { manager } = connection;
    if (options.provision) {
      await ensureTransportStream(manager, streamPolicy(ADVISORY_CAPTURE_STREAM, [transportAdvisoryStreamSubject], options, {
        maxAgeMs: 30 * 24 * 60 * 60 * 1_000,
        maxBytes: Math.max(options.maxBytes, 64 * 1024 * 1024),
        maxMessageBytes: 64 * 1024,
      }), true);
      assertAlive();
    }
    const leases = await openNatsKvForMachine(connection.js, "leases", options.machine, options.provision, connection.onStatus);
    assertAlive();
    const presence = await openNatsKvForMachine(connection.js, "presence", options.machine, options.provision, connection.onStatus);
    assertAlive();
    await ensureTransportStream(manager, streamPolicy(transportInboxStream(options.machine), [transportInboxStreamSubject(options.machine)], options, {
      maxAgeMs: 0,
    }), options.provision);
    assertAlive();
    const consumerPolicy = transportConsumerPolicy(options);
    const consumer = await ensureTransportConsumer(manager.consumers, consumerPolicy);
    assertAlive();
    if (consumer.config.description !== consumerPolicy.description) throw new TransportInvalidError("transport inbox owner description mismatch");
    let eventsPublisher: NatsEventsPublisher | undefined;
    if (options.events) {
      const { openNatsEventsPublisher } = await import("./index.js");
      assertAlive();
      // Fabric-store owns stream creation with admin credentials. Even mesh.nats.provision=true
      // cannot grant runtime creation: require the existing FABRIC_EV_<NORMALIZED_ORG> stream.
      // Its canonical layout/policy belongs to provision.ts; GitHub uses its separate GITHUB stream.
      eventsPublisher = await openNatsEventsPublisher(connection, { org: options.org!, provision: false });
      assertAlive();
    }
    return { connection, js: connection.js, manager, kvStores: { leases, presence },
      ...(eventsPublisher ? { eventsPublisher } : {}) };
  } catch (error) {
    await connection.close().catch(() => undefined);
    signal?.throwIfAborted();
    throw error;
  }
};

const authorizationFailure = (error: unknown): boolean => {
  let cause: unknown = error;
  for (let depth = 0; cause && depth < 6; depth += 1) {
    if (typeof cause === "object" && cause !== null) {
      const candidate = cause as { readonly name?: unknown; readonly code?: unknown; readonly cause?: unknown };
      if (candidate.name === "NatsAuthorizationError" || candidate.code === "FABRIC_NATS_AUTHORIZATION") return true;
      cause = candidate.cause;
    } else break;
  }
  return false;
};

class RebindingKvStore implements TransportKv {
  readonly ttlMs: number;
  constructor(
    ttlMs: number,
    private readonly current: () => NatsKvStore | undefined,
    private readonly waitForSession: (signal: AbortSignal) => Promise<NatsSession>,
    private readonly epoch: () => number,
    private readonly closed: () => boolean,
    private readonly unavailable: () => Error,
  ) { this.ttlMs = ttlMs; }

  async get<V>(key: string, signal?: AbortSignal): Promise<TransportKvEntry<V> | undefined> {
    const store = this.current();
    if (!store) throw this.unavailable();
    return store.get<V>(key, signal);
  }
  async put<V>(key: string, owner: string, value: V, ifRevision: number, signal?: AbortSignal): Promise<number> {
    const store = this.current();
    if (!store) throw this.unavailable();
    return store.put(key, owner, value, ifRevision, signal);
  }
  async delete(key: string, ifRevision: number, signal?: AbortSignal): Promise<boolean> {
    const store = this.current();
    if (!store) throw this.unavailable();
    return store.delete(key, ifRevision, signal);
  }
  async *watch<V>(prefix: string, signal: AbortSignal): AsyncIterable<import("../types.js").TransportKvEvent<V>> {
    let first = true;
    let lastEpoch = -1;
    let resetEmittedEpoch: number | undefined;
    while (!signal.aborted && !this.closed()) {
      const currentEpoch = this.epoch();
      const store = this.current();
      if (!first && (!store || currentEpoch !== lastEpoch) && resetEmittedEpoch !== currentEpoch) {
        resetEmittedEpoch = currentEpoch;
        yield { type: "reset" };
      }
      try { await this.waitForSession(signal); }
      catch (error) { if (signal.aborted || this.closed()) return; throw error; }
      if (signal.aborted || this.closed()) return;
      const bound = this.current();
      if (!bound) continue;
      lastEpoch = this.epoch();
      first = false;
      const iterator = bound.watch<V>(prefix, signal)[Symbol.asyncIterator]();
      let reset = false;
      try {
        while (!signal.aborted && !this.closed()) {
          const result = await iterator.next();
          if (signal.aborted || this.closed()) return;
          if (result.done) break;
          if (result.value.type === "reset") {
            reset = true;
            resetEmittedEpoch = this.epoch();
          } else if (result.value.type === "ready" && this.epoch() === lastEpoch) {
            // A ready from the newly verified store completes this reset generation.
            resetEmittedEpoch = undefined;
          }
          yield result.value;
        }
      } catch (error) {
        if (signal.aborted || this.closed()) return;
        if (this.epoch() !== lastEpoch || !this.current()) {
          if (!reset && resetEmittedEpoch !== this.epoch()) {
            resetEmittedEpoch = this.epoch();
            yield { type: "reset" };
          }
          continue;
        }
        throw error;
      } finally { await iterator.return?.(); }
      if (signal.aborted || this.closed()) return;
      if (reset || this.epoch() !== lastEpoch || !this.current()) {
        const nextEpoch = this.epoch();
        if (!reset && resetEmittedEpoch !== nextEpoch) {
          resetEmittedEpoch = nextEpoch;
          yield { type: "reset" };
        } else if (reset) {
          resetEmittedEpoch = nextEpoch;
        }
        continue;
      }
      return;
    }
  }
}

class NatsTransportImpl implements FabricTransport {
  readonly kind = "nats" as const;
  readonly self: TransportOwner;
  readonly #stream: string;
  readonly #options: ResolvedNatsOptions;
  readonly #closeController = new AbortController();
  #session: NatsSession | undefined;
  #closed = false;
  #closeTask: Promise<void> | undefined;
  readonly #onOwnerAbort = (): void => { void this.close().catch(() => undefined); };
  #terminalError: Error | undefined;
  #reconnectTask: Promise<void> | undefined;
  #epoch = 0;
  #inboxActive = false;
  #activeInbox: { readonly controller: AbortController; messages?: ConsumerMessages; queue?: Readable } | undefined;
  readonly #kvStores: Readonly<Record<"leases" | "presence", TransportKv>>;
  #placementTerminalStore: { readonly epoch: number; readonly store: NatsKvStore } | undefined;
  readonly #waiters = new Set<ReadyWaiter>();
  readonly #sessionChannels = new NatsSessionChannelManager();
  #eventPublisher: MeshEventPublisherBinding | undefined;

  private constructor(session: NatsSession, options: ResolvedNatsOptions) {
    this.#session = session;
    this.#stream = transportInboxStream(options.machine);
    this.#options = options;
    this.self = { machine: options.machine, hostId: options.hostId };
    this.#kvStores = {
      leases: new RebindingKvStore(LEASE_TTL_MS, () => this.#session?.kvStores.leases, (signal) => this.#waitForSession(signal), () => this.#epoch, () => this.#closed, () => this.#unavailable()),
      presence: new RebindingKvStore(PRESENCE_TTL_MS, () => this.#session?.kvStores.presence, (signal) => this.#waitForSession(signal), () => this.#epoch, () => this.#closed, () => this.#unavailable()),
    };
    this.#installSession(session);
    options.signal?.addEventListener("abort", this.#onOwnerAbort, { once: true });
  }

  static async open(rawOptions: OpenOptions): Promise<NatsTransportImpl> {
    rawOptions.signal?.throwIfAborted();
    const options = normalizeOptions(rawOptions);
    if (!options.hostId) throw new Error("hostId is required");
    if (typeof options.events !== "boolean") throw new Error("NATS transport events must be a boolean");
    if (options.events && (!options.org || !options.mesh)) throw new Error("NATS transport events require org and mesh");
    for (const [key, value] of Object.entries(options)) {
      if (["servers", "machine", "hostId", "name", "credentials", "ephemeralHost"].includes(key)) continue;
      if (typeof value === "number" && (!Number.isSafeInteger(value) || value <= 0)) throw new Error(`${key} must be positive`);
    }
    if (typeof options.ephemeralHost !== "boolean") throw new Error("ephemeralHost must be a boolean");
    if (!Number.isSafeInteger(options.ephemeralInactiveSeconds) || options.ephemeralInactiveSeconds < 300 || options.ephemeralInactiveSeconds > 86_400) {
      throw new Error("ephemeralInactiveSeconds must be an integer from 300 through 86400");
    }
    const transport = new NatsTransportImpl(await openNatsSession(options, options.signal), options);
    try {
      if (options.events) {
        transport.#eventPublisher = options.mesh!.bindEventPublisher(`nats:${options.org!}`, event => {
          const publisher = transport.#assertOpen().eventsPublisher;
          if (!publisher) throw new TransportUnavailableError("NATS event publisher is unavailable");
          return publisher.publish(event);
        }, event => {
          if (typeof event.id !== "string" || !validMessageId(event.id)) throw new TransportInvalidError("invalid NATS event id");
          if (event.topic.split(".").some(token => token.length === 0)) throw new TransportInvalidError("invalid NATS event topic");
        });
      }
    } catch (error) {
      await transport.close().catch(() => undefined);
      throw error;
    }
    // Cover an abort between acquisition and owner-listener registration.
    if (options.signal?.aborted) {
      await transport.close().catch(() => undefined);
      options.signal.throwIfAborted();
    }
    return transport;
  }

  #unavailable(): Error {
    return this.#terminalError ?? new TransportUnavailableError("NATS transport is temporarily unavailable");
  }

  #waitForSession(signal: AbortSignal): Promise<NatsSession> {
    if (this.#terminalError) return Promise.reject(this.#terminalError);
    if (this.#session && this.#session.connection.isVerified() &&
      !this.#session.connection.nc.isClosed() && !this.#session.connection.nc.isDraining()) return Promise.resolve(this.#session);
    if (this.#closed) return Promise.reject(new TransportUnavailableError("NATS transport is closed"));
    signal.throwIfAborted();
    return new Promise<NatsSession>((resolve, reject) => {
      const waiter: ReadyWaiter = { resolve, reject, signal };
      waiter.onAbort = () => { this.#waiters.delete(waiter); reject(signal.reason ?? this.#unavailable()); };
      signal.addEventListener("abort", waiter.onAbort, { once: true });
      this.#waiters.add(waiter);
    });
  }

  #resolveWaiters(session: NatsSession): void {
    if (!session.connection.isVerified()) return;
    for (const waiter of [...this.#waiters]) {
      this.#waiters.delete(waiter);
      if (waiter.onAbort) waiter.signal.removeEventListener("abort", waiter.onAbort);
      waiter.resolve(session);
    }
  }

  #rejectWaiters(error: Error): void {
    for (const waiter of [...this.#waiters]) {
      this.#waiters.delete(waiter);
      if (waiter.onAbort) waiter.signal.removeEventListener("abort", waiter.onAbort);
      waiter.reject(error);
    }
  }

  #installSession(session: NatsSession): void {
    this.#session = session;
    this.#sessionChannels.setConnection(session.connection.nc);
    session.unsubscribe = session.connection.onStatus((status) => {
      if (this.#closed || this.#session !== session) return;
      if (status.type === "disconnect") {
        // Native reconnect keeps this verified session and its ordered KV clients. Invalidate
        // Fabric-facing views/inbox before the client's next dial; the connection gate remains
        // closed until USER.INFO passes and the verified reconnect status is delivered.
        this.#epoch += 1;
        this.#activeInbox?.controller.abort();
        this.#sessionChannels.setConnection(undefined);
      } else if (status.type === "reconnect") {
        this.#sessionChannels.setConnection(session.connection.nc);
        this.#resolveWaiters(session);
        // A retained outbox row retries only after the native session was re-verified.
        void this.#eventPublisher?.retry();
      }
    });
    void session.connection.closed().then((error) => {
      if (authorizationFailure(error)) {
        this.#terminalError = error instanceof Error ? error : new Error("NATS authorization failed");
        this.#activeInbox?.controller.abort();
        this.#rejectWaiters(this.#terminalError);
      }
      this.#loseSession(session);
    });
    this.#resolveWaiters(session);
  }

  #loseSession(session: NatsSession): void {
    if (this.#closed || this.#session !== session) return;
    this.#session = undefined;
    this.#sessionChannels.setConnection(undefined);
    this.#epoch += 1;
    this.#activeInbox?.controller.abort();
    session.unsubscribe?.();
    void session.connection.close().catch(() => undefined);
    if (this.#terminalError) {
      this.#rejectWaiters(this.#terminalError);
      return;
    }
    this.#scheduleReconnect();
  }

  #scheduleReconnect(): void {
    if (this.#reconnectTask || this.#closed || this.#terminalError) return;
    // Preserve a bounded full reopen only for hard closes; ordinary broker restarts use the native verified session.
    this.#reconnectTask = this.#reconnectLoop().finally(() => {
      this.#reconnectTask = undefined;
      // A just-installed session can close before this loop has settled.
      if (!this.#session) this.#scheduleReconnect();
    });
  }

  async #reconnectLoop(): Promise<void> {
    for (let attempt = 0; !this.#closed && !this.#terminalError && !this.#session; attempt += 1) {
      const delay = Math.min(5_000, 100 * 2 ** Math.min(attempt, 6)) + Math.floor(Math.random() * 100);
      try { await sleep(delay, undefined, { signal: this.#closeController.signal }); }
      catch { return; }
      if (this.#closed || this.#closeController.signal.aborted || this.#terminalError || this.#session) return;
      try {
        const session = await openNatsSession(this.#options, this.#closeController.signal);
        if (this.#closed || this.#closeController.signal.aborted) { await session.connection.close().catch(() => undefined); return; }
        this.#installSession(session);
        return;
      } catch (error) {
        if (this.#closed || this.#closeController.signal.aborted) return;
        if (authorizationFailure(error)) {
          this.#terminalError = error instanceof Error ? error : new Error("NATS authorization failed");
          this.#activeInbox?.controller.abort();
          this.#rejectWaiters(this.#terminalError);
          return;
        }
      }
    }
  }

  #assertOpen(): NatsSession {
    if (this.#terminalError) throw this.#terminalError;
    if (this.#closed) throw new TransportUnavailableError("NATS transport is closed");
    const session = this.#session;
    if (!session || !session.connection.isVerified() || session.connection.nc.isClosed() || session.connection.nc.isDraining()) {
      if (session && (session.connection.nc.isClosed() || session.connection.nc.isDraining())) this.#loseSession(session);
      throw new TransportUnavailableError("NATS transport is temporarily unavailable");
    }
    return session;
  }

  #wire<T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): { session: NatsSession; resolved: TransportSent<T>["envelope"]; subject: string; bytes: Uint8Array } {
    const session = this.#assertOpen();
    signal?.throwIfAborted();
    if (!isTransportEnvelope(envelope)) throw new TransportInvalidError("invalid transport envelope");
    if (!validMessageId(envelope.id)) throw new TransportInvalidError("invalid transport message id");
    const committedAt = Date.now();
    // Evaluate a stamp before the serialization boundary so its original error is never
    // replaced by a JSON/UTF-8 error. The resolved body is then used for this publish.
    const body = typeof envelope.body === "function" ? (envelope.body as (at: number) => T)(committedAt) : envelope.body;
    let resolved: TransportSent<T>["envelope"];
    let bytes: Uint8Array;
    try {
      const json = JSON.stringify({ format: 1, sourceMachine: this.self.machine, envelope: { ...envelope, body, createdAt: committedAt } });
      bytes = utf8.encode(json);
      // Snapshot the actual wire representation once. Caller mutation while waiting for a
      // PubAck must not change the resolvedBody needed for a same-id uncertain retry.
      resolved = (JSON.parse(json) as { envelope: TransportSent<T>["envelope"] }).envelope;
    } catch (error) {
      throw new TransportInvalidError("transport envelope is not serializable", { cause: error });
    }
    const kind = resolved.kind === "command" ? "cmd" : "ack";
    const subject = transportInboxSubject(resolved.to.machine, resolved.to.hostId, kind, resolved.targetId, this.self.machine);
    if (bytes.byteLength > this.#options.maxMessageBytes) throw new TransportInvalidError(`transport envelope exceeds ${this.#options.maxMessageBytes} bytes`);
    return { session, resolved, subject, bytes };
  }

  async send<T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): Promise<TransportSent<T>> {
    const prepared = this.#wire(envelope, signal);
    let ack;
    try {
      ack = await prepared.session.js.publish(prepared.subject, prepared.bytes, {
        msgID: prepared.resolved.id,
        timeout: this.#options.requestTimeoutMs,
        expect: { streamName: this.#streamFor(prepared.resolved.to.machine) },
      });
    } catch (error) {
      throw publishFailure(error, prepared.resolved.id, prepared.resolved.body);
    }
    if (signal?.aborted) throw new TransportUncertainError(prepared.resolved.id, prepared.resolved.body, { cause: signal.reason });
    return { id: prepared.resolved.id, sequence: ack.seq, duplicate: ack.duplicate, envelope: prepared.resolved };
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
    const consumerPolicy = transportConsumerPolicy(this.#options);
    try {
      while (!signal.aborted && !this.#closed) {
        let session: NatsSession;
        try { session = await this.#waitForSession(signal); }
        catch (error) { if (this.#closed) return; throw error; }
        if (signal.aborted || this.#closed) return;
        const controller = new AbortController();
        const queue = new Readable({ objectMode: true, read() {} });
        let messages: ConsumerMessages | undefined;
        const end = (): void => { if (!queue.destroyed && !queue.readableEnded) queue.push(null); };
        const stop = (): void => { end(); void messages?.close(); };
        this.#activeInbox = { controller, queue };
        signal.addEventListener("abort", stop, { once: true });
        this.#closeController.signal.addEventListener("abort", stop, { once: true });
        controller.signal.addEventListener("abort", stop, { once: true });
        try {
          try {
          const info = await ensureTransportConsumer(session.manager.consumers, consumerPolicy);
          if (info.config.filter_subject !== consumerPolicy.filterSubject || info.config.description !== consumerPolicy.description) {
            throw new TransportInvalidError("transport inbox owner/filter mismatch");
          }
          const consumer = await session.js.consumers.get(this.#stream, consumerPolicy.durable);
          messages = await consumer.consume({ max_messages: 16, callback: (msg) => {
            if (!session.connection.isVerified()) return; // Native reconnect may replay SUBs before USER.INFO.
            if (!signal.aborted && !controller.signal.aborted && !this.#closed && this.#session === session && !queue.destroyed) queue.push(msg);
          } });
          this.#activeInbox = { controller, messages, queue };
          void messages.closed().then((error) => { if (error) queue.destroy(error); else end(); });
          try {
            for await (const msg of queue as AsyncIterable<JsMsg>) {
              if (signal.aborted || controller.signal.aborted || this.#closed || this.#session !== session) break;
              if (!session.connection.isVerified()) { controller.abort(); break; }
              let decoded;
              try { decoded = this.#decode(msg); }
              catch { await msg.term("malformed transport envelope"); continue; }
              const payload = decoded.envelope;
              const assertCurrentDelivery = (): void => {
                if (this.#terminalError) throw this.#terminalError;
                if (this.#closed) throw new TransportUnavailableError("NATS transport is closed");
                if (this.#session !== session || !session.connection.isVerified()) {
                  throw new TransportUnavailableError("NATS inbox delivery is suspended until USER.INFO re-verifies");
                }
              };
              let finished = false;
              const delivery: TransportDelivery<TransportEnvelope> = {
                payload,
                sourceMachine: decoded.sourceMachine,
                verification: "bridge",
                attempt: msg.info.deliveryCount,
                sequence: msg.info.streamSequence,
                working: () => { if (!finished && this.#session === session && session.connection.isVerified()) msg.working(); },
                ack: async () => {
                  if (finished) return;
                  assertCurrentDelivery();
                  const confirmed = await msg.ackAck();
                  if (!confirmed) throw new Error("transport broker ACK was not confirmed");
                  finished = true;
                },
                nak: (delayMs?: number) => { if (!finished) { assertCurrentDelivery(); msg.nak(delayMs); } },
                term: async (reason: string) => { if (!finished) { assertCurrentDelivery(); msg.term(reason); finished = true; } },
              };
              yield delivery;
            }
          } catch (error) {
            if (!controller.signal.aborted && !signal.aborted) throw error;
          }
          } catch (error) {
            if (this.#closed) return;
            if (!controller.signal.aborted && (session.connection.nc.isClosed() || session.connection.nc.isDraining())) this.#loseSession(session);
            if (!(controller.signal.aborted && !signal.aborted && !this.#closed && this.#session !== session)) throw error;
          }
        } finally {
          signal.removeEventListener("abort", stop);
          this.#closeController.signal.removeEventListener("abort", stop);
          controller.signal.removeEventListener("abort", stop);
          if (messages) await messages.close();
          queue.destroy();
          if (this.#activeInbox?.controller === controller) this.#activeInbox = undefined;
        }
        if (!signal.aborted && !this.#closed && this.#session !== session) continue;
        if (!signal.aborted && !this.#closed && controller.signal.aborted) continue;
        return;
      }
    } finally {
      this.#inboxActive = false;
      this.#activeInbox = undefined;
    }
  }

  kv(bucket: "leases" | "presence"): TransportKv {
    return this.#kvStores[bucket];
  }

  async writePlacementTerminal(record: PlacementTerminalRecord): Promise<void> {
    const { PlacementTerminalWriteError } = await import("../../placement/terminal.js");
    try {
      const session = this.#assertOpen();
      const epoch = this.#epoch;
      let current = this.#placementTerminalStore;
      if (!current || current.epoch !== epoch) {
        const store = await openNatsKv(session.js, {
          bucket: PLACEMENT_TERMINAL_BUCKET,
          ttlMs: PLACEMENT_TERMINAL_TTL_MS,
        }, this.#options.provision, session.connection.onStatus);
        if (this.#session !== session || this.#epoch !== epoch) throw new PlacementTerminalWriteError();
        current = { epoch, store };
        this.#placementTerminalStore = current;
      }
      await current.store.writePlacementTerminal(record);
    } catch (error) {
      if (error instanceof PlacementTerminalWriteError) throw error;
      throw new PlacementTerminalWriteError(error);
    }
  }

  openSessionChannel(identity: SessionChannelIdentity, options: SessionChannelOpenOptions): Promise<SessionChannel> {
    if (!this.#options.org) throw new Error("mesh.nats.org is required to open a Fabric session channel");
    if (!this.#session) throw this.#unavailable();
    return this.#sessionChannels.open({ ...identity, org: this.#options.org, hostId: this.self.hostId }, options);
  }

  async drainState(signal?: AbortSignal): Promise<{ pending: number; ackPending: number }> {
    const session = this.#assertOpen();
    signal?.throwIfAborted();
    const info = await session.manager.consumers.info(this.#stream, transportDurableName(this.self.hostId));
    signal?.throwIfAborted();
    return { pending: info.num_pending, ackPending: info.num_ack_pending };
  }

  close(): Promise<void> {
    if (this.#closed) return this.#closeTask ?? Promise.resolve();
    this.#closed = true;
    this.#options.signal?.removeEventListener("abort", this.#onOwnerAbort);
    this.#closeController.abort(this.#options.signal?.reason);
    this.#sessionChannels.close();
    this.#activeInbox?.controller.abort();
    this.#rejectWaiters(new TransportUnavailableError("NATS transport is closed"));
    const session = this.#session;
    this.#session = undefined;
    session?.unsubscribe?.();
    const reconnect = this.#reconnectTask;
    const eventsClosed = this.#eventPublisher?.close();
    this.#closeTask = (async () => {
      // Join the bounded in-flight attempt before closing its connection. Unsent/uncertain rows
      // remain durable for reload; close never republishes under a new local event id.
      try {
        await eventsClosed;
        if (session) {
          if (this.#options.ephemeralHost) {
            let timer: ReturnType<typeof setTimeout> | undefined;
            try {
              await Promise.race([
                session.manager.consumers.delete(this.#stream, transportDurableName(this.self.hostId)).then(() => undefined),
                new Promise<void>((resolve) => { timer = setTimeout(resolve, EPHEMERAL_DELETE_TIMEOUT_MS); }),
              ]);
            } catch { /* best-effort cleanup must not block shutdown */ }
            finally { if (timer) clearTimeout(timer); }
          }
          await session.connection.close();
        }
      } finally { if (reconnect) await reconnect; }
    })();
    return this.#closeTask;
  }
}

export function openNatsTransport(options: FabricTransportOptions): Promise<FabricTransport>;
export function openNatsTransport(options: NatsTransportOptions): Promise<FabricTransport>;
export function openNatsTransport(options: OpenOptions): Promise<FabricTransport> {
  return NatsTransportImpl.open(options);
}
