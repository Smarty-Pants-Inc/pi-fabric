import { createHash, randomBytes } from "node:crypto";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import { writeJsonAtomicAsync } from "../core/atomic-write.js";
import { AdmissionGate, WalGFrontierProvider, type AdmissionStatus } from "./admission.js";
import { RecordsArgumentError } from "./kinds.js";
import { LineReader, RecordsServiceError, wireError, type WireRequest, type WireResponse } from "./protocol.js";
import { migrate } from "./schema.js";
import { SharedFrontierProvider } from "./shared-frontier.js";
import { RecordStore, type ClientPool, type PageArgs, type RecordsPrincipal } from "./store.js";
import { RecordsWatchdog } from "./watchdog.js";
import { peerCredentials, processAlive, type PeerInfo } from "./peer.js";

/**
 * The records service (C10): the only process with database authority. It runs as the org's
 * `<org>-records` OS user, which owns the PostgreSQL cluster (a 0700 socket directory, peer
 * authentication for that user only), and serves the records calls on a unix socket that the
 * org's agents may connect to. Each call carries a token; the service derives the principal
 * from it, and the importer and mirror roles come from this service's own configuration.
 */

export interface RecordsServiceConfig {
  org: string;
  origin: string;
  /** The unix socket the service listens on. */
  socket: string;
  /** How the service connects: as records_service, over the cluster's private socket. */
  database: { host?: string; port?: number; database?: string; user?: string };
  /** How `migrate` connects: the cluster owner, only at install. */
  migration?: { host?: string; port?: number; database?: string; user?: string };
  /** Principal ids with each role; only the operator edits this file. */
  roles: { importer: string[]; mirror: string[]; relay: string[] };
  mirror: { enabled: boolean; repos?: string[] };
  admission: { targets: { name: string; command: string[]; env?: Record<string, string>; timeoutMs?: number }[]; alarmSeconds: number; refuseSeconds: number; refreshMs: number; segmentSize?: number };
  statusFile?: string;
  consumerLagSeconds: number;
}

const object = (value: unknown): Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {};
const text = (value: unknown): string | undefined => typeof value === "string" && value.trim() ? value.trim() : undefined;
const strings = (value: unknown): string[] => Array.isArray(value) ? value.map(text).filter((item): item is string => item !== undefined) : [];
const integer = (value: unknown, fallback: number, min: number, max: number): number =>
  typeof value === "number" && Number.isSafeInteger(value) ? Math.min(max, Math.max(min, value)) : fallback;
const connection = (value: unknown) => {
  const raw = object(value);
  return {
    ...(text(raw.host) ? { host: text(raw.host)! } : {}),
    ...(typeof raw.port === "number" ? { port: integer(raw.port, 5432, 1, 65_535) } : {}),
    ...(text(raw.database) ? { database: text(raw.database)! } : {}),
    ...(text(raw.user) ? { user: text(raw.user)! } : {}),
  };
};

export const normalizeServiceConfig = (input: unknown): RecordsServiceConfig => {
  const raw = object(input);
  const org = text(raw.org);
  const origin = text(raw.origin);
  const socket = text(raw.socket);
  if (!org || !origin || !socket) throw new Error("records service config needs org, origin and socket");
  const admission = object(raw.admission);
  const targets = (Array.isArray(admission.targets) ? admission.targets : []).flatMap((entry) => {
    const target = object(entry);
    const name = text(target.name);
    const command = strings(target.command);
    if (!name || command.length === 0) return [];
    const env = Object.fromEntries(Object.entries(object(target.env)).filter((pair): pair is [string, string] => typeof pair[1] === "string"));
    return [{ name, command, ...(Object.keys(env).length ? { env } : {}), ...(typeof target.timeoutMs === "number" ? { timeoutMs: integer(target.timeoutMs, 60_000, 1_000, 600_000) } : {}) }];
  });
  const alarmSeconds = integer(admission.alarmSeconds, 120, 1, 86_400);
  const roles = object(raw.roles);
  const mirror = object(raw.mirror);
  return {
    org, origin, socket,
    database: connection(raw.database),
    ...(raw.migration !== undefined ? { migration: connection(raw.migration) } : {}),
    roles: { importer: strings(roles.importer), mirror: strings(roles.mirror), relay: strings(roles.relay) },
    mirror: { enabled: mirror.enabled === true, ...(Array.isArray(mirror.repos) ? { repos: strings(mirror.repos) } : {}) },
    admission: {
      targets, alarmSeconds,
      refuseSeconds: Math.max(alarmSeconds, integer(admission.refuseSeconds, 300, 1, 86_400)),
      refreshMs: integer(admission.refreshMs, 30_000, 1_000, 3_600_000),
      ...(typeof admission.segmentSize === "number" ? { segmentSize: integer(admission.segmentSize, 16 * 1024 * 1024, 1024 * 1024, 1024 * 1024 * 1024) } : {}),
    },
    ...(text(raw.statusFile) ? { statusFile: text(raw.statusFile)! } : {}),
    consumerLagSeconds: integer(raw.consumerLagSeconds, 120, 1, 86_400),
  };
};

/** Ids a session or actor may register for itself: a Main's `session:<uuid>`, an actor's hex id. */
const SELF_REGISTERED = /^(session:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{32})$/;
const hashToken = (token: string): string => createHash("sha256").update(token).digest("hex");
const newToken = (): string => randomBytes(32).toString("base64url");

const openPool = async (options: RecordsServiceConfig["database"]): Promise<ClientPool> => {
  const { default: pg } = await import("pg");
  const pool = new pg.Pool({ ...options, max: 8, idleTimeoutMillis: 30_000, application_name: "records-service" });
  pool.on("error", () => undefined);
  return pool as unknown as ClientPool;
};

/** Apply migrations as the cluster owner (install only). */
export const migrateService = async (config: RecordsServiceConfig, pool?: ClientPool): Promise<number> => {
  const owner = pool ?? await openPool(config.migration ?? config.database);
  try {
    const client = await owner.connect();
    try { return await migrate(client); } finally { client.release(); }
  } finally {
    if (!pool) await owner.end?.();
  }
};

/** Issue an operator principal's token (for the importer or the mirror). Prints nothing; returns it. */
export type OperatorRole = "importer" | "mirror" | "relay";
export const OPERATOR_ROLES: readonly OperatorRole[] = ["importer", "mirror", "relay"];

/**
 * Issue an operator principal's token (the installer, as the records user). The id is in the
 * reserved operator namespace (never a session or actor id, which only registration creates),
 * and the role is recorded with it in the same statement.
 */
export const issuePrincipal = async (config: RecordsServiceConfig, id: string, role: OperatorRole, name?: string, pool?: ClientPool): Promise<{ id: string; token: string }> => {
  if (!/^[A-Za-z0-9._@:-]{1,128}$/.test(id) || SELF_REGISTERED.test(id)) throw new Error(`invalid operator principal id ${JSON.stringify(id)}: a session or actor id registers itself`);
  if (!OPERATOR_ROLES.includes(role)) throw new Error(`invalid operator role ${JSON.stringify(role)}`);
  const owner = pool ?? await openPool(config.database);
  const store = new RecordStore(owner, { org: config.org, origin: config.origin });
  try {
    const token = newToken();
    await store.transaction((client) => client.query(
      "INSERT INTO principals (id, name, token_hash, issued_by, role) VALUES ($1, $2, $3, 'operator', $4)", [id, name ?? id, hashToken(token), role]));
    return { id, token };
  } finally {
    if (!pool) await owner.end?.();
  }
};

type Handler = (principal: RecordsPrincipal, args: Record<string, unknown>, signal: AbortSignal, peer?: PeerInfo) => Promise<unknown>;

/** One principal's token seen from a second live process within this window raises the theft alarm. */
const TOKEN_REUSE_WINDOW_MS = 60 * 60_000;
export interface TokenReuseAlert { principal: string; pids: number[]; cmdlines: string[]; at: string }

export class RecordsServer {
  readonly store: RecordStore;
  readonly gate: AdmissionGate;
  readonly watchdog: RecordsWatchdog;
  readonly statusFile: string | undefined;
  readonly #life = new AbortController();
  readonly #principals = new Map<string, RecordsPrincipal>();
  readonly #handlers: Record<string, Handler>;
  #server: net.Server | undefined;
  readonly #connections = new Set<net.Socket>();
  #statusWrite: Promise<void> = Promise.resolve();
  #admission: AdmissionStatus | undefined;
  /** Each principal's processes, by pid, and when each last called. */
  readonly #seen = new Map<string, Map<number, { at: number; cmdline?: string }>>();
  readonly #alerts: TokenReuseAlert[] = [];

  private constructor(readonly config: RecordsServiceConfig, pool: ClientPool, readonly options: { now?: () => number } = {}) {
    this.statusFile = config.statusFile;
    this.gate = new AdmissionGate({
      providers: config.admission.targets.map((target) => new SharedFrontierProvider(new WalGFrontierProvider(target.name, target.command, {
        ...(target.env ? { env: target.env } : {}), ...(target.timeoutMs ? { timeoutMs: target.timeoutMs } : {}),
        ...(config.admission.segmentSize ? { segmentSize: config.admission.segmentSize } : {}),
      }), () => this.store, config.admission.refreshMs)),
      alarmSeconds: config.admission.alarmSeconds,
      refuseSeconds: config.admission.refuseSeconds,
      ...(options.now ? { now: options.now } : {}),
      onStatus: (status) => this.#writeStatus(status),
    });
    this.store = new RecordStore(pool, { org: config.org, origin: config.origin, mirror: config.mirror, admission: this.gate });
    this.watchdog = new RecordsWatchdog({
      store: this.store,
      check: async (signal) => {
        if (!this.gate.enabled) return;
        await this.gate.refresh(signal);
        signal?.throwIfAborted();
        this.gate.evaluate(await this.store.transaction((client) => this.store.admissionInput(client, this.gate.frontier()), "", signal));
      },
      intervalMs: config.admission.refreshMs,
      signal: this.#life.signal,
      ...(options.now ? { now: options.now } : {}),
    });
    const store = this.store;
    const ownArgs = (args: Record<string, unknown>) => args;
    // Publication and alarm state belongs to the reserved relay role, never to ordinary tokens (F9).
    const relayOnly = (handler: Handler): Handler => (principal, args, signal) => {
      if (!principal.relay) throw new RecordsServiceError("only the records relay may claim or complete publications and alarms", "RECORD_FORBIDDEN");
      return handler(principal, args, signal);
    };
    this.#handlers = {
      whoami: async (principal) => ({ id: principal.id, importer: principal.importer === true, mirror: principal.mirror === true }),
      status: async (_principal, _args, signal) => this.status(signal),
      append: (principal, args, signal, peer) => store.append(principal, args.args, { signal, ...(peer ? { peer } : {}) }),
      read: (principal, args, signal) => store.read(principal, args.args, { signal }),
      get: (principal, args, signal) => store.get(principal, args.args, { signal }),
      fold: (principal, args, signal) => store.fold(principal, args.args, { signal }),
      list: (principal, args, signal) => store.list(principal, args.args, { signal }),
      page: (_principal, args, signal) => store.page(pageArgs(ownArgs(args), store.origin), signal),
      byIds: (_principal, args, signal) => store.byIds(ids(args.ids), signal),
      // A consumer's cursor is always the caller's own.
      openConsumer: (principal, args, signal) => store.openConsumer(principal.id, names(args.names), signal),
      saveConsumer: (principal, args, signal) => store.saveConsumer(principal.id, count(args.after), pending(args.pending), signal),
      // A claim belongs to the principal that took it: only it acks, fails or releases its rows.
      claimPublications: relayOnly((principal, args, signal) => store.claimPublications(Math.min(500, Math.max(1, count(args.limit))), signal, principal.id)),
      ackPublication: relayOnly((principal, args, signal) => store.ackPublication({ claimId: uuid(args.claimId), recordId: uuid(args.recordId) }, count(args.meshSequence), signal, principal.id)),
      failPublication: relayOnly((principal, args, signal) => store.failPublication({ claimId: uuid(args.claimId), recordId: uuid(args.recordId) }, String(args.error ?? "").slice(0, 500), signal, principal.id)),
      releasePublications: relayOnly((principal, args, signal) => store.releasePublications(uuid(args.claimId), ids(args.recordIds), signal, principal.id)),
      unpublished: (_principal, _args, signal) => store.unpublished(signal),
      lagging: (_principal, args, signal) => store.lagging(count(args.lagMs), this.#now(), signal),
      claimAlarm: relayOnly((_principal, args, signal) => this.#claimAlarm(args.key, count(args.realarmMs), signal)),
    };
  }

  static async open(config: RecordsServiceConfig, options: { pool?: ClientPool; now?: () => number } = {}): Promise<RecordsServer> {
    const pool = options.pool ?? await openPool(config.database);
    const server = new RecordsServer(config, pool, options.now ? { now: options.now } : {});
    // A crash between a commit and its recovery bound leaves one unbounded record: bound it now.
    await server.store.transaction((client) => server.store.fillBounds(client, "all"));
    server.watchdog.start();
    return server;
  }

  #now(): number { return this.options.now?.() ?? Date.now(); }

  /**
   * An alarm claim is honored only while its condition holds (a consumer lags now; the archive
   * gate is in that state now), with at least the default re-alarm window, so a caller cannot
   * silence an alarm ahead of time or for longer than one window.
   */
  async #claimAlarm(key: unknown, realarmMs: number, signal: AbortSignal): Promise<boolean> {
    if (typeof key !== "string") throw new RecordsArgumentError("alarm key must be a string");
    const window = Math.max(realarmMs, 10 * 60_000);
    const lag = /^consumer-lag:(.{1,200})$/.exec(key);
    if (lag) {
      const lagging = await this.store.lagging(this.config.consumerLagSeconds * 1000, this.#now(), signal);
      if (!lagging.some((entry) => entry.consumer === lag[1])) return false;
      return this.store.claimAlarm(key, this.#now(), window, signal);
    }
    const archive = /^archive-lag:(alarm|refuse)$/.exec(key);
    if (archive) {
      if (this.gate.status()?.state !== archive[1]) return false;
      return this.store.claimAlarm(key, this.#now(), window, signal);
    }
    throw new RecordsArgumentError("unknown alarm key");
  }

  get signal(): AbortSignal { return this.#life.signal; }

  async status(signal?: AbortSignal): Promise<Record<string, unknown>> {
    const frontier = await this.store.page({ after: Number.MAX_SAFE_INTEGER - 1, limit: 1, origin: this.store.origin }, signal);
    return {
      org: this.store.org, origin: this.store.origin, frontier: frontier.frontier, unpublished: await this.store.unpublished(signal),
      admission: this.gate.status() ?? { state: this.gate.enabled ? "unknown" : "disabled" },
      ...(this.statusFile ? { statusFile: this.statusFile } : {}),
    };
  }

  #holds(row: { id: string; issued_by: string; role: string | null }, role: OperatorRole): boolean {
    return row.issued_by === "operator" && row.role === role && this.config.roles[role].includes(row.id);
  }

  /** Register a session's or actor's own participant id (the first claim wins). */
  async register(id: unknown, name: unknown, signal: AbortSignal = this.#life.signal, nonce?: unknown): Promise<{ id: string; token: string }> {
    if (typeof id !== "string" || !SELF_REGISTERED.test(id)) {
      throw new RecordsServiceError("only a session (session:<uuid>) or an actor (its 32-hex id) registers itself; other principals are issued by the operator", "RECORD_PRINCIPAL_INVALID");
    }
    if (typeof nonce !== "string" || !/^[A-Za-z0-9_-]{32,128}$/.test(nonce)) {
      throw new RecordsServiceError("registration needs an enrollment nonce the client saved first", "RECORD_INVALID");
    }
    const token = newToken();
    const enrolled = await this.store.transaction(async (client) => {
      const inserted = (await client.query(
        "INSERT INTO principals (id, name, token_hash, nonce_hash, issued_by) VALUES ($1, $2, $3, $4, 'register') ON CONFLICT (id) DO NOTHING",
        [id, typeof name === "string" ? name.slice(0, 128) : null, hashToken(token), hashToken(nonce)])).rowCount === 1;
      if (inserted) return true;
      // Only the same nonce re-enrolls: the client that registered first, retrying after a lost response.
      return (await client.query<{ ok: boolean }>("SELECT principal_reenroll($1, $2, $3) AS ok", [id, hashToken(nonce), hashToken(token)])).rows[0]!.ok;
    }, "", signal);
    if (!enrolled) throw new RecordsServiceError(`records principal ${id} is already registered; use its credential`, "RECORD_PRINCIPAL_TAKEN");
    this.#principals.clear();
    return { id, token };
  }

  /** The principal a token names, with roles from this service's configuration only. */
  async authenticate(token: unknown): Promise<RecordsPrincipal> {
    if (typeof token !== "string" || !token) throw new RecordsServiceError("records call needs a token", "RECORD_UNAUTHENTICATED");
    const hash = hashToken(token);
    const cached = this.#principals.get(hash);
    if (cached) return cached;
    const row = await this.store.transaction(async (client) =>
      (await client.query<{ id: string; name: string | null; issued_by: string; role: string | null }>("SELECT id, name, issued_by, role FROM principals WHERE token_hash = $1", [hash])).rows[0], "", this.#life.signal);
    if (!row) throw new RecordsServiceError("records token is not known to this service", "RECORD_UNAUTHENTICATED");
    const principal: RecordsPrincipal = {
      id: row.id, ...(row.name ? { name: row.name } : {}),
      // A role needs the service config's grant AND the installer's issue for that role.
      importer: this.#holds(row, "importer"), mirror: this.#holds(row, "mirror"), relay: this.#holds(row, "relay"),
    };
    this.#principals.set(hash, principal);
    return principal;
  }

  async listen(socketPath = this.config.socket): Promise<void> {
    await removeStaleSocket(socketPath);
    const server = net.createServer((socket) => this.#serve(socket));
    this.#server = server;
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(socketPath, () => { server.off("error", reject); resolve(); });
    });
    // The directory's mode (0750, group of the org's agents) decides who may connect.
    fs.chmodSync(socketPath, 0o660);
  }

  #serve(socket: net.Socket): void {
    this.#connections.add(socket);
    socket.setEncoding("utf8");
    const calls = new Map<number, AbortController>();
    // The kernel's view of who is on the other end, asked once per connection.
    const peer = peerCredentials(socket);
    const send = (response: WireResponse) => { if (!socket.destroyed) socket.write(`${JSON.stringify(response)}\n`); };
    const reader = new LineReader((line) => {
      // A malformed frame ends this connection only; nothing in it may throw past here.
      const request = parseRequest(line);
      if (!request) { socket.destroy(); return; }
      if (request.method === "cancel") {
        const target = object(request.args).target;
        if (typeof target === "number") calls.get(target)?.abort(new Error("call cancelled by the client"));
        return;
      }
      // An id already in flight would detach the first call from its cancellation: refused.
      if (calls.has(request.id)) {
        send({ id: request.id, ok: false, error: { message: `request id ${request.id} is already in flight`, code: "RECORD_DUPLICATE_REQUEST" } });
        return;
      }
      const call = new AbortController();
      calls.set(request.id, call);
      void this.#dispatch(request, AbortSignal.any([call.signal, this.#life.signal]), peer)
        .then((result) => send({ id: request.id, ok: true, result: result ?? null }), (error: unknown) => send({ id: request.id, ok: false, error: wireError(error) }))
        .finally(() => calls.delete(request.id));
    }, () => socket.destroy());
    socket.on("data", (chunk: string) => {
      try { reader.push(chunk); } catch { socket.destroy(); }
    });
    // A client that goes away cancels its calls: nothing it started commits later.
    socket.on("close", () => {
      this.#connections.delete(socket);
      for (const call of calls.values()) call.abort(new Error("records client disconnected"));
    });
    socket.on("error", () => undefined);
  }

  async #dispatch(request: WireRequest, signal: AbortSignal, peerLookup?: Promise<PeerInfo | undefined>): Promise<unknown> {
    // async: any throw below, argument checks included, becomes this call's error response.
    const args = object(request.args);
    if (request.method === "hello") return { org: this.store.org, origin: this.store.origin, protocol: 1 };
    if (request.method === "register") return this.register(args.id, args.name, signal, args.nonce);
    const handler = Object.hasOwn(this.#handlers, request.method) ? this.#handlers[request.method] : undefined;
    if (!handler) throw new RecordsServiceError(`unknown records method ${JSON.stringify(String(request.method).slice(0, 64))}`, "RECORD_UNKNOWN_METHOD");
    const principal = await this.authenticate(request.token);
    const peer = await peerLookup;
    if (peer) this.#notePeer(principal.id, peer);
    signal.throwIfAborted();
    return handler(principal, args, signal, peer);
  }

  /** The theft alarm: one token in use from two live processes within an hour. */
  #notePeer(principal: string, peer: PeerInfo): void {
    const now = this.#now();
    const seen = this.#seen.get(principal) ?? new Map<number, { at: number; cmdline?: string }>();
    this.#seen.set(principal, seen);
    for (const [pid, entry] of seen) if (now - entry.at > TOKEN_REUSE_WINDOW_MS || (pid !== peer.pid && !processAlive(pid))) seen.delete(pid);
    const fresh = !seen.has(peer.pid);
    seen.set(peer.pid, { at: now, ...(peer.cmdline ? { cmdline: peer.cmdline } : {}) });
    if (!fresh || seen.size < 2) return;
    const alert: TokenReuseAlert = {
      principal, pids: [...seen.keys()], cmdlines: [...seen.values()].map((entry) => entry.cmdline ?? "?"), at: new Date(now).toISOString(),
    };
    this.#alerts.push(alert);
    if (this.#alerts.length > 50) this.#alerts.shift();
    process.stderr.write(`records service: ALARM token of ${principal} used by ${alert.pids.length} live processes: ${alert.pids.join(", ")}\n`);
    this.#flushStatus();
  }

  /** Token-reuse alerts so far (newest last). */
  alerts(): readonly TokenReuseAlert[] {
    return this.#alerts;
  }

  #writeStatus(status: AdmissionStatus): void {
    this.#admission = status;
    this.#flushStatus();
  }

  #flushStatus(): void {
    const file = this.statusFile;
    if (!file) return;
    const alarms = this.#alerts.map((alert) => `token of ${alert.principal} used by live processes ${alert.pids.join(", ")} at ${alert.at}`);
    const record = {
      org: this.store.org, origin: this.store.origin, updatedAt: new Date(this.#now()).toISOString(),
      ...(this.#admission ? { admission: this.#admission } : {}),
      ...(alarms.length ? { alarm: alarms.at(-1), tokenReuse: this.#alerts } : {}),
    };
    this.#statusWrite = this.#statusWrite.then(async () => {
      await fs.promises.mkdir(path.dirname(file), { recursive: true, mode: 0o755 });
      // Readable by the factory check (another user); it holds no secret.
      await writeJsonAtomicAsync(file, record, { space: 2, newline: true, mode: 0o644, dirMode: 0o755 });
    }).catch(() => undefined);
  }

  /** Apply a new role policy (SIGHUP): cached principals are dropped and re-read. */
  reloadRoles(roles: RecordsServiceConfig["roles"]): void {
    (this.config as { roles: RecordsServiceConfig["roles"] }).roles = roles;
    this.#principals.clear();
  }

  /** Drop every client connection (their calls in flight are cancelled); clients reconnect. */
  disconnectAll(): void {
    for (const socket of this.#connections) socket.destroy();
  }

  /** Stop accepting calls, cancel those in flight (their connections roll back), and end the pool. Bounded. */
  async close(timeoutMs = 5_000): Promise<void> {
    this.watchdog.stop();
    this.#life.abort(new Error("records service closed"));
    const bounded = (work: Promise<unknown>) => Promise.race([work.catch(() => undefined), new Promise((resolve) => setTimeout(resolve, timeoutMs).unref?.())]);
    const server = this.#server;
    if (server) {
      for (const socket of this.#connections) socket.destroy();
      await bounded(new Promise<void>((resolve) => server.close(() => resolve())));
    }
    await bounded(this.watchdog.idle());
    await bounded(this.#statusWrite);
    await bounded(this.store.close());
  }
}

const count = (value: unknown): number => {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) throw new RecordsArgumentError("expected a non-negative integer");
  return value;
};
const uuid = (value: unknown): string => {
  if (typeof value !== "string" || !/^[0-9a-f-]{36}$/i.test(value)) throw new RecordsArgumentError("expected a record id");
  return value.toLowerCase();
};
const ids = (value: unknown): string[] => (Array.isArray(value) ? value : []).slice(0, 500).map(uuid);
const names = (value: unknown): string[] => (Array.isArray(value) ? value : []).filter((name): name is string => typeof name === "string" && name.length <= 256).slice(0, 32);
const pending = (value: unknown): { through: number; ids: string[] } | null => {
  if (value === null || value === undefined) return null;
  const raw = object(value);
  return { through: count(raw.through), ids: ids(raw.ids) };
};
const pageArgs = (args: Record<string, unknown>, origin: string): PageArgs => ({
  after: count(args.after), limit: Math.min(500, Math.max(1, count(args.limit))), origin: typeof args.origin === "string" ? args.origin : origin,
  ...(typeof args.ref === "string" ? { ref: args.ref } : {}),
  ...(typeof args.kind === "string" ? { kind: args.kind as PageArgs["kind"] & string } : {}),
  ...(Array.isArray(args.to) ? { to: names(args.to) } : {}),
  ...(typeof args.exceptAuthor === "string" ? { exceptAuthor: args.exceptAuthor } : {}),
});

/** A request envelope, or undefined for anything else (never throws). */
const parseRequest = (line: string): WireRequest | undefined => {
  let value: unknown;
  try { value = JSON.parse(line); } catch { return undefined; }
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  const request = value as Record<string, unknown>;
  if (typeof request.id !== "number" || !Number.isSafeInteger(request.id) || typeof request.method !== "string") return undefined;
  if (request.token !== undefined && typeof request.token !== "string") return undefined;
  if (request.args !== undefined && (typeof request.args !== "object" || request.args === null || Array.isArray(request.args))) return undefined;
  return { id: request.id, method: request.method, ...(typeof request.token === "string" ? { token: request.token } : {}), ...(request.args !== undefined ? { args: request.args } : {}) };
};

/** A socket file left by a crashed service is removed; a live one is refused. */
const removeStaleSocket = async (socketPath: string): Promise<void> => {
  if (!fs.existsSync(socketPath)) return;
  const live = await new Promise<boolean>((resolve) => {
    const probe = net.connect(socketPath);
    probe.once("connect", () => { probe.destroy(); resolve(true); });
    probe.once("error", () => resolve(false));
  });
  if (live) throw new Error(`records service already listening on ${socketPath}`);
  fs.unlinkSync(socketPath);
};
