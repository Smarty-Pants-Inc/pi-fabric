import { createConnection, type Socket } from "node:net";
import { connect as tlsConnect, TLSSocket, type ConnectionOptions as TlsConnectionOptions } from "node:tls";
import {
  AuthorizationError, NatsConnectionImpl, ProtocolHandler, UserAuthenticationExpiredError,
  deferred, setTransportFactory,
  type ConnectionOptions, type Deferred, type NatsConnection, type NodeConnectionOptions, type Publisher, type Server,
} from "@nats-io/transport-node";
import { NodeTransport, VERSION, nodeResolveHost } from "@nats-io/transport-node/lib/node_transport.js";

type Phase = "DNS" | "TCP" | "TLS" | "INFO" | "connect/handshake" | "auth";
const cancelled = (): Error => new DOMException("NATS connection closed", "AbortError");
const asError = (value: unknown): Error => value instanceof Error ? value : cancelled();

/** Observe native work, but settle promptly when its owner closes. The late work remains observed. */
const interruptible = async <T>(work: Promise<T>, signal: AbortSignal): Promise<T> => {
  let abort!: () => void;
  const stopped = new Promise<never>((_, reject) => {
    abort = () => reject(signal.reason);
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
  });
  try { return await Promise.race([work, stopped]); }
  finally { signal.removeEventListener("abort", abort); }
};

/**
 * Pinned @nats-io/{transport-node,nats-core} 3.4.0 adapter, not a second retry engine.
 *
 * Upstream protocol.js:399-424 allocates transport in private prepare(); :468-505 owns
 * the handshake deadline; :507-638 resolves addresses and runs the native dialLoop.
 * NodeTransport's pending TCP/TLS sockets are local until an await completes and its
 * close() ignores pre-INFO sockets (node_transport.js:33-107,200-294,398-405).
 * We replace only allocation/socket-ownership seams. Selection, backoff, auth-abort,
 * handshake, SUB replay, and reconnect policy remain upstream. No factory captures
 * a caller: each protocol explicitly constructs its own transport on every dial.
 * Recheck these seams and the real-socket tests before upgrading either package.
 */
class OwnedNodeTransport extends NodeTransport {
  readonly stopped = new AbortController();
  readonly sockets = new Set<Socket>();
  phase: Phase = "TCP";
  constructor(private readonly owner: AbortSignal) { super(); }

  private check(): void {
    this.owner.throwIfAborted();
    this.stopped.signal.throwIfAborted();
    if (this.done) throw cancelled();
  }

  override async connect(hp: Server, options: NodeConnectionOptions): Promise<void> {
    this.check();
    await interruptible(super.connect(hp, options), this.stopped.signal);
    this.check();
    this.phase = "connect/handshake";
  }

  /** Record ownership synchronously, including TCP connect and TLS handshake in flight. */
  private own<T extends Socket>(socket: T, event: "connect" | "secureConnect"): Promise<T> {
    this.socket = socket;
    this.sockets.add(socket);
    socket.setNoDelay(true);
    return new Promise<T>((resolve, reject) => {
      let error: Error | undefined;
      const cleanup = (): void => {
        socket.off(event, ready);
        socket.off("error", failed);
        socket.off("close", closed);
        this.stopped.signal.removeEventListener("abort", abort);
      };
      const failed = (cause: Error): void => { error = cause; };
      const closed = (): void => { cleanup(); reject(error ?? cancelled()); };
      const abort = (): void => { cleanup(); socket.destroy(); reject(this.stopped.signal.reason); };
      const ready = (): void => {
        try {
          this.check();
          if (socket instanceof TLSSocket && !socket.authorized) throw asError(socket.authorizationError);
          cleanup();
          resolve(socket);
        } catch (cause) { cleanup(); socket.destroy(); reject(cause); }
      };
      socket.on("error", failed);
      socket.once("close", closed);
      socket.once(event, ready);
      this.stopped.signal.addEventListener("abort", abort, { once: true });
      if (this.stopped.signal.aborted) abort();
    });
  }

  override dial(hp: { hostname: string; port: number }): Promise<Socket> {
    this.check();
    this.phase = "TCP";
    return this.own(createConnection(hp.port, hp.hostname), "connect");
  }

  override async peekInfo(): ReturnType<NodeTransport["peekInfo"]> {
    this.check();
    this.phase = "INFO";
    return interruptible(super.peekInfo(), this.stopped.signal);
  }

  override async loadFile(file: string): ReturnType<NodeTransport["loadFile"]> {
    this.check();
    const bytes = await interruptible(super.loadFile(file), this.stopped.signal);
    this.check();
    return bytes;
  }

  private async tlsOptions(): Promise<TlsConnectionOptions> {
    this.check();
    this.phase = "TLS";
    const certs = typeof this.options.tls === "object" && this.options.tls !== null
      ? await this.loadClientCerts() : undefined;
    // A certificate read can finish after abort/timeout. Never create a late TLS socket.
    this.check();
    return {
      servername: this.tlsName, ...this.options.tls,
      ...(certs && certs.cert !== undefined ? { cert: certs.cert } : {}),
      ...(certs && certs.key !== undefined ? { key: certs.key } : {}),
      ...(certs && certs.ca !== undefined ? { ca: certs.ca } : {}),
      rejectUnauthorized: true,
    };
  }

  override async tlsFirst(hp: { hostname: string; port: number }): Promise<TLSSocket> {
    const options = await this.tlsOptions();
    this.check();
    return this.own(tlsConnect(hp.port, hp.hostname, options), "secureConnect");
  }

  override async startTLS(): Promise<TLSSocket> {
    const options = await this.tlsOptions();
    this.check();
    return this.own(tlsConnect({ ...options, socket: this.socket }), "secureConnect");
  }

  override async close(error?: Error): Promise<void> {
    this.stopped.abort(error ?? cancelled());
    // Only a fully connected transport may notify native disconnected(). Failed dials
    // are already accounted for by dialLoop; notifying twice would start overlapping loops.
    const closed = this.connected ? super.close(error) : Promise.resolve();
    for (const socket of this.sockets) socket.destroy();
    this.sockets.clear();
    this.done = true;
    this.signal.resolve();
    await closed;
  }

  override discard(): void {
    this.stopped.abort(cancelled());
    for (const socket of this.sockets) socket.destroy();
    this.sockets.clear();
    super.discard();
    this.signal.resolve();
  }
}

// The JS method is overridable, but 3.4.0 declares prepare private. Confine that
// declaration bypass here; all other protocol members remain checked against upstream.
interface ProtocolSeams extends Omit<ProtocolHandler, "prepare" | "dial" | "_doDial" | "close"> {
  prepare(): Deferred<void>;
  dial(server: Server): Promise<void>;
  _doDial(server: Parameters<ProtocolHandler["_doDial"]>[0]): Promise<void>;
  close(error?: Error): Promise<void>;
}
const ProtocolBase = ProtocolHandler as unknown as {
  new(options: ConnectionOptions, publisher: Publisher): ProtocolSeams;
};

class OwnedProtocol extends ProtocolBase {
  readonly stop = new AbortController();
  private nextWarning = 0;
  private suppressed = 0;
  private addressDial = false;

  constructor(options: ConnectionOptions, publisher: Publisher) {
    super(options, publisher);
    // Native close dereferences transport even when DNS hasn't completed yet.
    this.transport = new OwnedNodeTransport(this.stop.signal);
  }

  override prepare(): Deferred<void> {
    this.stop.signal.throwIfAborted();
    this.transport.discard();
    delete this.info;
    this.resetOutbound();
    const pong = deferred<void>();
    void pong.catch(() => undefined);
    this.pongs.unshift(pong);
    this.connectError = (error) => pong.reject(error);
    const transport = new OwnedNodeTransport(this.stop.signal);
    this.transport = transport;
    void transport.closed().then(async () => {
      this.connected = false;
      if (!this.isClosed() && !this.stop.signal.aborted) {
        await this.disconnected(transport.closeError || this.lastError);
      }
    });
    return pong;
  }

  override async dial(server: Server): Promise<void> {
    // Upstream _doDial resumes here after DNS and between address alternatives.
    // The native loop also resumes here after any backoff/extra-selection delay.
    this.stop.signal.throwIfAborted();
    this.addressDial = true;
    try {
      await super.dial(server);
      this.stop.signal.throwIfAborted();
    } finally {
      if (this.stop.signal.aborted) this.heartbeats.cancel();
    }
  }

  override async _doDial(server: Parameters<ProtocolHandler["_doDial"]>[0]): Promise<void> {
    this.stop.signal.throwIfAborted();
    this.addressDial = false;
    try { await super._doDial(server); }
    catch (cause) {
      if (!this.stop.signal.aborted) this.warn(cause);
      throw cause; // Native dialLoop, not this adapter, decides whether/when to retry.
    }
  }

  private warn(cause: unknown): void {
    const now = Date.now();
    if (now < this.nextWarning) { this.suppressed += 1; return; }
    this.nextWarning = now + 1_000;
    let phase: Phase = this.addressDial ? (this.transport as OwnedNodeTransport).phase : "DNS";
    let code = "Error";
    let errno: string | undefined;
    // Never log messages, endpoints, file paths, raw protocol frames, or JWTs.
    for (let error = cause, depth = 0; error && depth < 6; depth += 1) {
      if (error instanceof AuthorizationError || error instanceof UserAuthenticationExpiredError) phase = "auth";
      if (typeof error !== "object") break;
      const fields = error as { code?: unknown; errno?: unknown; name?: unknown; cause?: unknown };
      const safe = (value: unknown): string | undefined => typeof value === "string" && /^[A-Za-z0-9_-]{1,64}$/.test(value) ? value : undefined;
      code = safe(fields.code) ?? (code === "Error" ? safe(fields.name) ?? code : code);
      if (typeof fields.errno === "number" && Number.isSafeInteger(fields.errno)) errno = String(fields.errno);
      else errno = safe(fields.errno) ?? errno;
      error = fields.cause;
    }
    console.warn(`NATS connection attempt failed phase=${phase} code=${code}${errno ? ` errno=${errno}` : ""}${this.suppressed ? ` suppressed=${this.suppressed}` : ""}`);
    this.suppressed = 0;
  }

  override close(error?: Error): Promise<void> {
    this.abortReconnect = true;
    this.stop.abort(error ?? cancelled());
    this.dialDelay?.cancel();
    // Destroy pending sockets synchronously even if native close joins status iterators.
    void this.transport.close(error);
    return super.close(error);
  }
}

/** Internal-only, cancelable bootstrap of the pinned native client. */
export const connectFirstNats = async (options: NodeConnectionOptions, signal?: AbortSignal): Promise<NatsConnection> => {
  signal?.throwIfAborted();
  if (VERSION !== "3.4.0") throw new Error("NATS first-connect adapter requires transport-node 3.4.0");
  // Same generic, owner-free factory setup as transport-node/lib/connect.js:25-30.
  // Our prepare() never obtains an owned transport from this module-global factory.
  setTransportFactory({ factory: () => new NodeTransport(), dnsResolveFn: nodeResolveHost });
  // The constructor parses options but is private only in the TS declaration.
  const Connection = NatsConnectionImpl as unknown as { new(options: NodeConnectionOptions): NatsConnectionImpl };
  const nc = new Connection(options);
  const protocol = new OwnedProtocol(nc.options, nc);
  nc.protocol = protocol as unknown as ProtocolHandler;
  const abort = (): void => { void protocol.close(asError(signal?.reason)).catch(() => undefined); };
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) abort();
  try {
    await interruptible(protocol.dialLoop(), protocol.stop.signal);
    signal?.throwIfAborted();
    // Upstream's concrete info getter includes undefined while its public interface
    // spells it optional; the native implementation has this same declaration mismatch.
    return nc as unknown as NatsConnection;
  } catch (cause) {
    await protocol.close(asError(cause));
    signal?.throwIfAborted();
    throw cause;
  } finally { signal?.removeEventListener("abort", abort); }
};
