import { createHash, randomBytes } from "node:crypto";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import type { RecordsAnchor, RecordsVerifyResult } from "./chain.js";
import { AdmissionGate, WalGFrontierProvider, type AdmissionStatus } from "./admission.js";
import { AnchorExport } from "./anchor-export.js";
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
  /** Public append-only anchors; only the service's protected config chooses this path/cadence. */
  anchorExport?: { directory: string; intervalMs: number };
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
  const statusFile = text(raw.statusFile);
  const anchorExport = object(raw.anchorExport);
  // Existing installations already configure a public status directory: publish beneath it
  // without requiring a new principal or an installer to overwrite their protected config.
  const exportDirectory = text(anchorExport.directory) ?? (statusFile ? path.join(path.dirname(statusFile), "anchors") : undefined);
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
    ...(statusFile ? { statusFile } : {}),
    ...(exportDirectory ? { anchorExport: { directory: path.resolve(exportDirectory), intervalMs: integer(anchorExport.intervalMs, 300_000, 1_000, 86_400_000) } } : {}),
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
  // A checked-out client whose connection drops emits "error" too: without a listener that crashes
  // the process. The failure still reaches the caller as its rejected query.
  pool.on("connect", (client) => { client.on("error", () => undefined); });
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

/** The anchor and the chain check, straight from the database as the service's role (the CLI). */
const withStore = async <T>(config: RecordsServiceConfig, pool: ClientPool | undefined, work: (store: RecordStore) => Promise<T>): Promise<T> => {
  const owner = pool ?? await openPool(config.database);
  try { return await work(new RecordStore(owner, { org: config.org, origin: config.origin })); } finally {
    if (!pool) await owner.end?.();
  }
};
const OPERATOR: RecordsPrincipal = { id: "operator" };
export const anchorService = (config: RecordsServiceConfig, pool?: ClientPool): Promise<RecordsAnchor> =>
  withStore(config, pool, (store) => store.anchor(OPERATOR, {}));
export const verifyService = (config: RecordsServiceConfig, anchors: unknown, pool?: ClientPool): Promise<RecordsVerifyResult> =>
  withStore(config, pool, (store) => store.verify(OPERATOR, { anchors }));

/** Issue an operator principal's token (for the importer or the mirror). Prints nothing; returns it. */
export type OperatorRole = "importer" | "mirror" | "relay";
export const OPERATOR_ROLES: readonly OperatorRole[] = ["importer", "mirror", "relay"];

/**
 * Issue an operator principal's token (the installer, as the records user). The id is in the
 * reserved operator namespace (never a session or actor id, which only registration creates),
 * and the role is recorded with it in the same statement.
 */
export const issuePrincipal = async (config: RecordsServiceConfig, id: string, role: OperatorRole, name?: string, pool?: ClientPool, reissue = false, presetToken?: string): Promise<{ id: string; token: string }> => {
  if (!/^[A-Za-z0-9._@:-]{1,128}$/.test(id) || SELF_REGISTERED.test(id)) throw new Error(`invalid operator principal id ${JSON.stringify(id)}: a session or actor id registers itself`);
  if (!OPERATOR_ROLES.includes(role)) throw new Error(`invalid operator role ${JSON.stringify(role)}`);
  const owner = pool ?? await openPool(config.database);
  const store = new RecordStore(owner, { org: config.org, origin: config.origin });
  try {
    const token = presetToken ?? newToken();
    // reissue: an issuance interrupted after its commit (the credential file never written) is
    // recovered by rotating that operator principal's token. Only the same id AS AN OPERATOR WITH
    // THE SAME ROLE is rotated; a registered principal or another role is never touched.
    const written = await store.transaction(async (client) => {
      const inserted = (await client.query(
        "INSERT INTO principals (id, name, token_hash, issued_by, role) VALUES ($1, $2, $3, 'operator', $4) ON CONFLICT (id) DO NOTHING",
        [id, name ?? id, hashToken(token), role])).rowCount === 1;
      if (inserted || !reissue) return inserted;
      return (await client.query<{ ok: boolean }>("SELECT principal_reissue($1, $2, $3) AS ok", [id, role, hashToken(token)])).rows[0]!.ok;
    });
    if (!written) throw new Error(`operator principal ${id} already exists${reissue ? " as another kind or role" : "; pass --reissue to rotate its token"}`);
    return { id, token };
  } finally {
    if (!pool) await owner.end?.();
  }
};

/**
 * Take an exclusive flock(2) on `file` (created 0600, never followed, owned by this user) and return its fd;
 * closing the fd, or the process's death, releases it.
 */
// ponytail: util-linux flock(1) locks the open file description it inherits as fd 3, which this process keeps
// open after the child exits; no native addon, and the wait does not block the event loop.
// #1720: the wait is bounded (`flock -w`), and on Linux `setpriv --pdeathsig KILL` kills the helper when this
// process dies, so a killed issuer never leaves a waiter behind. Without setpriv only the deadline bounds it.
export const lockFile = async (file: string, waitSeconds = 120): Promise<number> => {
  const fd = fs.openSync(file, fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_NOFOLLOW, 0o600);
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.uid !== process.getuid?.()) throw new Error(`${file} is not a regular file owned by this user`);
    const { spawn } = await import("node:child_process");
    const flock = ["flock", "-x", "-w", String(waitSeconds), "3"];
    const run = (argv: string[]) => new Promise<number | null>((resolve, reject) => {
      const child = spawn(argv[0]!, argv.slice(1), { stdio: ["ignore", "ignore", "inherit", fd] });
      child.on("error", reject);
      child.on("exit", (status) => resolve(status));
    });
    const code = process.platform === "linux"
      ? await run(["setpriv", "--pdeathsig", "KILL", ...flock]).catch((error: NodeJS.ErrnoException) => {
        if (error.code === "ENOENT") return run(flock);
        throw error;
      })
      : await run(flock);
    // flock(1) exits 1 when -w expires.
    if (code === 1) throw new Error(`timed out after ${waitSeconds}s waiting for lock ${file}`);
    if (code !== 0) throw new Error(`cannot lock ${file} (flock exited ${code})`);
    return fd;
  } catch (error) {
    fs.closeSync(fd);
    throw error;
  }
};

/**
 * Issue (or, with reissue, rotate) an operator credential into `out`, never publishing a revoked
 * token and never losing a committed one. Under a per-principal advisory lock held until the file
 * is published (so concurrent issues for one principal run one at a time):
 *  1. a leftover `<out>.pending` from an interrupted run is published if the database holds its
 *     token, and discarded otherwise;
 *  2. the new credential goes to `<out>.pending` (0600, fixed name); the file, its directory and
 *     every ancestor up to the first one this user does not own are fsynced on every issue, so the
 *     pending pathname survives a host crash before the database can hold its token (F5, #119 F2);
 *  3. the database is changed ON THE SESSION THAT HOLDS THE LOCK (S6): if that session is lost, its
 *     rotation is lost with it, so no other run can hold the lock while this change may still commit;
 *  4. `<out>.pending` is renamed over `out`, and the directory fsynced.
 * Every step runs under an exclusive flock(2) on `<out>.lock` (0600, this user's, never followed), taken
 * before the database lock and released after publication or cleanup, or by the kernel when the process
 * dies. A stale issuer (its session lost) therefore never touches a successor's `<out>.pending` (#119 F1).
 * A database failure that certainly did not commit removes `<out>.pending` and leaves `out` and its
 * token as they were; after a failed COMMIT (outcome unknown) the pending file is kept and the call
 * fails, so the next run's recovery publishes it if the database holds its token. Without
 * reissue an existing `out` is refused before the database is touched.
 */
export const issueCredentialFile = async (
  config: RecordsServiceConfig, id: string, role: OperatorRole, out: string,
  options: { name?: string; reissue?: boolean; pool?: ClientPool; afterCommit?: () => Promise<void>; verifyOnly?: boolean } = {},
): Promise<"published-pending" | "verified" | "issued"> => {
  const outDir = path.dirname(out);
  fs.mkdirSync(outDir, { recursive: true, mode: 0o700 });
  // #119 F1: every change to `<out>.pending` happens under this file lock, held from before recovery until
  // after publication or cleanup. The kernel drops it only when this process closes it or dies, so an issuer
  // whose database session is lost can still finish (or clean up) its own pending file, and a successor waits.
  const fileLock = await lockFile(`${out}.lock`);
  let owner: ClientPool | undefined;
  let lock: Awaited<ReturnType<ClientPool["connect"]>>;
  try {
    owner = options.pool ?? await openPool(config.database);
    lock = await owner.connect();
  } catch (error) {
    if (!options.pool) await owner?.end?.();
    fs.closeSync(fileLock);
    throw error;
  }
  // A lost session rejects its query; the client's later "error" event must not crash the process.
  (lock as { on?: (event: string, listener: () => void) => unknown }).on?.("error", () => undefined);
  let lockBroken = false;
  // S6: the rotation's transaction runs on the lock's own session, never on another pooled one.
  const session: ClientPool = {
    connect: async () => ({
      query: lock.query.bind(lock),
      release: (destroy) => { if (destroy) lockBroken = true; },
    }),
  };
  const pending = `${out}.pending`;
  const fsyncDir = (dir: string): void => {
    const fd = fs.openSync(dir, "r");
    try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  };
  const readToken = (file: string): unknown => {
    try { return (JSON.parse(fs.readFileSync(file, "utf8")) as { token?: unknown }).token; } catch { return undefined; }
  };
  const dropPending = (): void => { fs.rmSync(pending, { force: true }); };
  try {
    await lock.query("SET ROLE fabric_records_writer");
    await lock.query("SELECT pg_advisory_lock(hashtextextended($1, 0))", [`fabric-records:issue:${id}`]);
    // F5/#119 F2: every directory up to the first one this user does not own (a root-owned base) is fsynced on
    // EVERY issue, before the database change: a retry after a failed first issue syncs the directories it left.
    const uid = process.getuid?.();
    const syncDirs: string[] = [];
    for (let d = outDir; ; d = path.dirname(d)) {
      syncDirs.push(d);
      if (d === path.dirname(d) || fs.statSync(d).uid !== uid) break;
    }
    const storedHash = async (): Promise<string | undefined> =>
      (await lock.query<{ token_hash: string }>("SELECT token_hash FROM principals WHERE id = $1 AND issued_by = 'operator' AND role = $2", [id, role])).rows[0]?.token_hash;
    const publish = (): void => {
      fs.renameSync(pending, out);
      fsyncDir(outDir);
    };
    // 1. Recover an interrupted run (under the lock just taken): its pending token is the live one exactly when the database holds it.
    let recovered = false;
    if (fs.existsSync(pending)) {
      const token = readToken(pending);
      if (typeof token === "string" && hashToken(token) === await storedHash()) { publish(); recovered = true; }
      else dropPending();
    }
    // verify: the canonical file must hold the token the database holds; nothing is issued.
    if (options.verifyOnly) {
      const token = readToken(out);
      const live = await storedHash();
      if (typeof token !== "string" || live === undefined || hashToken(token) !== live) {
        throw new Error(`${out} does not hold the live token of ${id} (${role}); run the installer's issue with --reissue`);
      }
      return recovered ? "published-pending" : "verified";
    }
    if (!options.reissue && fs.existsSync(out)) throw new Error(`${out} exists; pass --reissue to rotate its token`);
    // 2. The new credential is on disk before the database can make it the live one.
    const token = newToken();
    const fd = fs.openSync(pending, "wx", 0o600);
    try {
      fs.writeFileSync(fd, `${JSON.stringify({ id, token, role, issuedBy: "installer" })}\n`);
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    try {
      for (const d of syncDirs) fsyncDir(d);
    } catch (error) {
      dropPending();
      throw error;
    }
    // 3. The database change, on the lock's session.
    try {
      await issuePrincipal(config, id, role, options.name, session, options.reissue === true, token);
    } catch (error) {
      // S2/F4: a failed COMMIT may have committed. Its pending token is kept for the next run's
      // recovery (step 8's verify publishes it if the database holds it). It is removed only when
      // the change certainly did not commit (an error before COMMIT was sent); under the file lock
      // the pending file is still this issuer's (#119 F1).
      if ((error as { commitUncertain?: boolean }).commitUncertain === true) {
        throw new Error(`issue ${id}: the database outcome is unknown (${error instanceof Error ? error.message : String(error)}); ${pending} kept; outcome unknown; rerun to reconcile`);
      }
      dropPending();
      throw error;
    }
    await options.afterCommit?.();
    // 4. Publish.
    publish();
    return "issued";
  } finally {
    // The session goes back to a caller's pool as it came: no lock, no role.
    if (!lockBroken) await lock.query("SELECT pg_advisory_unlock_all()").then(() => lock.query("RESET ROLE")).catch(() => { lockBroken = true; });
    lock.release(lockBroken);
    if (!options.pool) await owner.end?.();
    fs.closeSync(fileLock);
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
  readonly #handlers: Record<string, Handler>;
  #server: net.Server | undefined;
  readonly #connections = new Set<net.Socket>();
  #statusWrite: Promise<void> = Promise.resolve();
  #admission: AdmissionStatus | undefined;
  /** Each principal's processes, by pid, and when each last called. */
  readonly #seen = new Map<string, Map<number, { at: number; cmdline?: string }>>();
  readonly #alerts: TokenReuseAlert[] = [];
  #verifying = false;
  readonly #anchorExport: AnchorExport | undefined;
  #anchorCheckedAt: number | undefined;
  #lastAnchor: RecordsAnchor | undefined;
  #anchorError: string | undefined;

  private constructor(readonly config: RecordsServiceConfig, pool: ClientPool, readonly options: { now?: () => number } = {}) {
    this.statusFile = config.statusFile;
    this.#anchorExport = config.anchorExport ? new AnchorExport(config.anchorExport.directory, (file) => lockFile(file, 5)) : undefined;
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
        // Publication is independent of archive admission and needs no socket principal.
        await this.#publishAnchor(signal).catch(() => undefined); // failure is exposed in status
        signal?.throwIfAborted();
        if (!this.gate.enabled) return;
        await this.gate.refresh(signal);
        signal?.throwIfAborted();
        this.gate.evaluate(await this.store.transaction((client) => this.store.admissionInput(client, this.gate.frontier()), "", signal));
      },
      intervalMs: Math.min(config.admission.refreshMs, config.anchorExport?.intervalMs ?? config.admission.refreshMs),
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
      anchor: (principal, args, signal) => store.anchor(principal, args.args, { signal }),
      // One verify at a time, and no queue (#118 S3): a scan reads the whole table, so while one runs
      // another caller gets a retryable RECORD_BUSY at once and nothing of its request is kept.
      verify: async (principal, args, signal) => {
        if (this.#verifying) throw new RecordsServiceError("records.verify is already running; retry in a few seconds", "RECORD_BUSY", true);
        this.#verifying = true;
        try { return await store.verify(principal, args.args, { signal }); } finally { this.#verifying = false; }
      },
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
    try {
      // A crash between a commit and its recovery bound leaves one unbounded record: bound it now.
      await server.store.transaction((client) => server.store.fillBounds(client, "all"));
      await server.#publishAnchor(server.signal);
      server.watchdog.start();
      return server;
    } catch (error) {
      await server.close();
      throw error;
    }
  }

  #now(): number { return this.options.now?.() ?? Date.now(); }

  #exportStatus(): { anchorExport?: { directory: string; intervalMs: number; last?: RecordsAnchor; error?: string } } {
    return this.config.anchorExport ? { anchorExport: {
      ...this.config.anchorExport,
      ...(this.#lastAnchor ? { last: this.#lastAnchor } : {}),
      ...(this.#anchorError ? { error: this.#anchorError } : {}),
    } } : {};
  }

  async #publishAnchor(signal?: AbortSignal): Promise<void> {
    if (!this.#anchorExport || !this.config.anchorExport) return;
    signal?.throwIfAborted();
    const now = this.#now();
    if (this.#anchorCheckedAt !== undefined && now - this.#anchorCheckedAt < this.config.anchorExport.intervalMs) return;
    try {
      const anchor = await this.store.anchor(OPERATOR, {}, { ...(signal ? { signal } : {}) });
      signal?.throwIfAborted();
      this.#lastAnchor = await this.#anchorExport.publish(anchor);
      this.#anchorCheckedAt = now;
      this.#anchorError = undefined;
      this.#flushStatus();
    } catch (error) {
      this.#anchorError = error instanceof Error ? error.message : String(error);
      this.#flushStatus();
      if (!signal?.aborted) process.stderr.write(`records service: anchor export failed: ${this.#anchorError}\n`);
      throw error;
    }
  }

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
      ...this.#exportStatus(),
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
    return { id, token };
  }

  /** The principal a token names, with roles from this service's configuration only. */
  async authenticate(token: unknown): Promise<RecordsPrincipal> {
    if (typeof token !== "string" || !token) throw new RecordsServiceError("records call needs a token", "RECORD_UNAUTHENTICATED");
    const hash = hashToken(token);
    // No cache: every call reads the current token hash (one indexed lookup), so a reissued or
    // removed token is refused at once by the running service, and a role reload applies at once.
    const row = await this.store.transaction(async (client) =>
      (await client.query<{ id: string; name: string | null; issued_by: string; role: string | null }>("SELECT id, name, issued_by, role FROM principals WHERE token_hash = $1", [hash])).rows[0], "", this.#life.signal);
    if (!row) throw new RecordsServiceError("records token is not known to this service", "RECORD_UNAUTHENTICATED");
    const principal: RecordsPrincipal = {
      id: row.id, ...(row.name ? { name: row.name } : {}),
      // A role needs the service config's grant AND the installer's issue for that role.
      importer: this.#holds(row, "importer"), mirror: this.#holds(row, "mirror"), relay: this.#holds(row, "relay"),
    };
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
      ...this.#exportStatus(),
      ...(this.#admission ? { admission: this.#admission } : {}),
      ...(alarms.length ? { alarm: alarms.at(-1), tokenReuse: this.#alerts } : {}),
    };
    this.#statusWrite = this.#statusWrite.then(async () => {
      await fs.promises.mkdir(path.dirname(file), { recursive: true, mode: 0o755 });
      await writeStatusFile(file, `${JSON.stringify(record, null, 2)}\n`);
    }).catch(() => undefined);
  }

  /** Apply a new role policy (SIGHUP); authentication reads it on the next call. */
  reloadRoles(roles: RecordsServiceConfig["roles"]): void {
    (this.config as { roles: RecordsServiceConfig["roles"] }).roles = roles;
  }

  /** How many client connections are open (status, and tests that must see a disconnect land). */
  get connectionCount(): number {
    return this.#connections.size;
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

/**
 * The status file is readable by the factory check (another user) and holds no secret: 0644
 * whatever the process umask (the unit's UMask=0007 would make it 0640). The mode is set with
 * fchmod on the new file's descriptor before the atomic rename. Secrets are never written here.
 */
export const writeStatusFile = async (file: string, text: string): Promise<void> => {
  await fs.promises.mkdir(path.dirname(file), { recursive: true, mode: 0o755 });
  const temp = `${file}.${process.pid}.${randomBytes(4).toString("hex")}.tmp`;
  const handle = await fs.promises.open(temp, "wx", 0o600);
  try {
    await handle.writeFile(text);
    await handle.chmod(0o644);
    await handle.sync();
  } finally {
    await handle.close();
  }
  try {
    await fs.promises.rename(temp, file);
  } catch (error) {
    await fs.promises.rm(temp, { force: true });
    throw error;
  }
};

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
