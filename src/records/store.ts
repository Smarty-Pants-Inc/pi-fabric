import type { AdmissionGate } from "./admission.js";
import {
  MIRRORED_KINDS, RecordsArgumentError, parseRef, parseRepo, payloadHash, recordTopic, validateAppend,
  type AppendArgs, type RecordKind,
} from "./kinds.js";
import { WRITER_ROLE, type SqlClient } from "./schema.js";

/**
 * The record layer over the org's PostgreSQL database (smarty-dev#754 §1-5).
 *
 * `append` is one transaction under a per-org advisory lock, so commit order equals `seq` and a
 * reader by cursor never sees a later record before an earlier one (C4). It checks the caller's
 * idempotency key (C3), the archive frontier (C2), writes the record, its outbox row for a
 * mirrored kind, and its publication row, and answers only after COMMIT (synchronous_commit on).
 */

/** Who calls (C13): the authenticated principal, never a payload field. */
export interface RecordsPrincipal {
  /** The authenticated participant id (a mesh identity id); the author of what it appends. */
  id: string;
  /** Display name only; never authority. */
  name?: string;
  /** The GitHub importer role: may set `author` on an imported record, with data.via. */
  importer?: boolean;
  /** The mirror role: may append record.mirror. */
  mirror?: boolean;
}

export interface RecordReceipt {
  id: string;
  sequence: number;
  origin: string;
  topic: string;
  ref: string;
  key: string;
  createdAt: number;
}

export interface RecordEnvelope {
  id: string;
  org: string;
  origin: string;
  sequence: number;
  ref: string;
  topic: string;
  kind: RecordKind;
  /** The author: a participant id, or `github:<login>` for an imported record. */
  from: string;
  fromName?: string;
  createdAt: number;
  text?: string;
  data: Record<string, unknown>;
  supersedes?: string;
  key: string;
}

export interface RecordsReadArgs { after?: number; limit?: number; origin?: string; ref?: string; kind?: RecordKind; to?: string }
export interface RecordsPage { records: RecordEnvelope[]; next: number; frontier: number; origin: string }

export interface RecordStatusFold { id: string; at: number; text?: string; state?: string; eta?: unknown; waitOn?: string; name?: string }
export interface RecordFold {
  title?: string; body?: string; owner?: string; acceptance?: string; labels?: string[]; nextAction?: string; stage?: string;
  open: boolean;
  statuses: Record<string, RecordStatusFold>;
  decisions: RecordEnvelope[];
  openAsks: RecordEnvelope[];
  links: RecordEnvelope[];
  mirror: Record<string, Record<string, unknown>>;
}
export interface RecordsGetArgs { ref: string; after?: number; limit?: number }
export interface RecordsGetResult { ref: string; state: RecordFold; history: RecordEnvelope[]; next?: number }

export interface RecordsListArgs { org?: string; repo?: string; open?: boolean; owner?: string; hasOpenAsk?: boolean; updatedSince?: number; limit?: number; after?: string }
export interface RecordsListItem {
  ref: string; title?: string; owner?: string; stage?: string; open: boolean; updatedAt: number;
  statuses: Record<string, { at: number; state?: string; eta?: unknown }>;
  openAsks: { id: string; to?: string; at: number }[];
}
export interface RecordsListResult { items: RecordsListItem[]; next?: string }

/**
 * The records calls, whoever serves them. The local service answers them over the Node's socket;
 * the remote records endpoint (C10, a follow-up) authenticates a per-principal token, derives the
 * principal from it, and calls the same backend. A payload never names its principal.
 */
export interface RecordsBackend {
  append(principal: RecordsPrincipal, args: unknown, options?: RecordsCallOptions): Promise<RecordReceipt>;
  read(principal: RecordsPrincipal, args: unknown, options?: RecordsCallOptions): Promise<RecordsPage>;
  get(principal: RecordsPrincipal, args: unknown, options?: RecordsCallOptions): Promise<RecordsGetResult>;
  list(principal: RecordsPrincipal, args: unknown, options?: RecordsCallOptions): Promise<RecordsListResult>;
}

/** The caller's lifetime: once aborted, no further step of its call runs and nothing it started commits. */
export interface RecordsCallOptions { signal?: AbortSignal }

/**
 * Kinds whose records belong to their author: only the same author may supersede one. An issue is
 * shared (a stage command by the owner or a person supersedes it, C14).
 */
const AUTHOR_OWNED_SUPERSEDE = new Set<RecordKind>(["status", "comment", "decision", "ask", "answer", "handoff", "link", "close", "reopen", "mirror"]);

export interface ConsumerState { after: number; pending: { through: number; ids: string[] } | null }
export interface ClaimedPublication {
  recordId: string; sequence: number; topic: string; recipient: string | null; kind: string; ref: string; from: string; key: string; text: string | null; createdAt: number;
}
export interface ConsumerLag { consumer: string; after: number; oldestAt: number; count: number }
export interface PageArgs { after: number; limit: number; origin: string; ref?: string; kind?: RecordKind; to?: readonly string[]; exceptAuthor?: string }

/**
 * What the inbox, the relay and the watchdog need, whoever serves it: the store in process
 * (tests, and the records service itself) or the records service over its socket (Fabric).
 */
export interface RecordsOps {
  readonly org: string;
  readonly origin: string;
  page(args: PageArgs, signal?: AbortSignal): Promise<RecordsPage>;
  byIds(ids: readonly string[], signal?: AbortSignal): Promise<RecordEnvelope[]>;
  openConsumer(consumer: string, names: readonly string[], signal?: AbortSignal): Promise<ConsumerState>;
  saveConsumer(consumer: string, after: number, pending: ConsumerState["pending"], signal?: AbortSignal): Promise<void>;
  claimPublications(limit: number, signal?: AbortSignal): Promise<ClaimedPublication[]>;
  ackPublication(recordId: string, meshSequence: number, signal?: AbortSignal): Promise<void>;
  failPublication(recordId: string, error: string, signal?: AbortSignal): Promise<void>;
  releasePublications(recordIds: readonly string[], signal?: AbortSignal): Promise<void>;
  unpublished(signal?: AbortSignal): Promise<number>;
  lagging(lagMs: number, now: number, signal?: AbortSignal): Promise<ConsumerLag[]>;
  claimAlarm(key: string, now: number, realarmMs: number, signal?: AbortSignal): Promise<boolean>;
}

/** How long a relay's claim on unpublished nudges lasts before another relay may take them. */
const PUBLICATION_LEASE_SECONDS = 30;

export class RecordKeyConflictError extends Error {
  readonly code = "RECORD_KEY_CONFLICT";
  readonly retryable = false;
  constructor(readonly key: string, readonly existing: RecordReceipt) {
    super(`record key ${JSON.stringify(key)} was already used for a different payload (record ${existing.id}); use a new key for a new record`);
  }
}

export interface PooledClient extends SqlClient { release(error?: Error | boolean): void }

/** A pool connection that an abort gives back as soon as it arrives, instead of to a dead caller. */
const connect = (pool: ClientPool, signal: AbortSignal | undefined): Promise<PooledClient> => {
  if (!signal) return pool.connect();
  signal.throwIfAborted();
  return new Promise((resolve, reject) => {
    const pending = pool.connect();
    // The connection, when it comes, goes straight back (below): the caller is gone.
    const onAbort = () => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    pending.then((client) => {
      signal.removeEventListener("abort", onAbort);
      if (signal.aborted) client.release();
      else resolve(client);
    }, (error: unknown) => {
      signal.removeEventListener("abort", onAbort);
      reject(error);
    });
  });
};

/** Bounds on one transaction's waits (the advisory lock included), so no call waits forever. */
const LOCK_TIMEOUT = "30s";
const STATEMENT_TIMEOUT = "60s";
export interface ClientPool { connect(): Promise<PooledClient>; end?(): Promise<void> }

export interface RecordStoreOptions {
  org: string;
  origin: string;
  /** The role every transaction works as (SET LOCAL ROLE); default fabric_records_writer. */
  role?: string;
  mirror?: { enabled: boolean; repos?: readonly string[] };
  admission?: AdmissionGate;
  /** Called after each committed append (the publication relay's trigger). */
  onCommitted?: () => void;
}

const ROLE_NAME = /^[a-z_][a-z0-9_]{0,62}$/;
const MAX_PAGE = 500;

interface RecordRow {
  id: string; org: string; origin: string; seq: string; ref: string; kind: RecordKind; author: string; author_name: string | null;
  created_at: Date; text: string | null; data: Record<string, unknown>; supersedes: string | null; key: string; payload_hash: string;
}

const RECORD_COLUMNS = "id, org, origin, seq, ref, kind, author, author_name, created_at, text, data, supersedes, key, payload_hash";

const envelope = (row: RecordRow): RecordEnvelope => ({
  id: row.id, org: row.org, origin: row.origin, sequence: Number(row.seq), ref: row.ref, topic: recordTopic(parseRef(row.ref)),
  kind: row.kind, from: row.author, ...(row.author_name ? { fromName: row.author_name } : {}), createdAt: row.created_at.getTime(),
  ...(row.text !== null ? { text: row.text } : {}), data: row.data, ...(row.supersedes ? { supersedes: row.supersedes } : {}), key: row.key,
});

const receipt = (row: Pick<RecordRow, "id" | "seq" | "origin" | "ref" | "key" | "created_at">): RecordReceipt => ({
  id: row.id, sequence: Number(row.seq), origin: row.origin, topic: recordTopic(parseRef(row.ref)), ref: row.ref, key: row.key, createdAt: row.created_at.getTime(),
});

const isObject = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);

const checkKeys = (name: string, args: Record<string, unknown>, allowed: readonly string[]): void => {
  const extra = Object.keys(args).filter((key) => !allowed.includes(key));
  if (extra.length) throw new RecordsArgumentError(`records.${name}: unknown field ${extra.map((key) => JSON.stringify(key)).join(", ")}; allowed: ${allowed.join(", ")}`);
};
const optionalInteger = (name: string, value: unknown, min: number, max: number): number | undefined => {
  if (value === undefined) return undefined;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < min || value > max) throw new RecordsArgumentError(`${name} must be an integer from ${min} to ${max}`);
  return value;
};
const optionalString = (name: string, value: unknown): string | undefined => {
  if (value === undefined) return undefined;
  if (typeof value !== "string" || !value.trim() || value.length > 512) throw new RecordsArgumentError(`${name} must be a non-empty string`);
  return value;
};

export class RecordStore implements RecordsBackend, RecordsOps {
  readonly org: string;
  readonly origin: string;
  readonly #role: string;

  constructor(readonly pool: ClientPool, readonly options: RecordStoreOptions) {
    if (!options.org.trim() || !options.origin.trim()) throw new Error("records: org and origin are required");
    this.org = options.org;
    this.origin = options.origin;
    this.#role = options.role ?? WRITER_ROLE;
    if (!ROLE_NAME.test(this.#role)) throw new Error(`records: invalid role name ${JSON.stringify(this.#role)}`);
  }

  /**
   * One transaction as the writer role; rolled back on any error. An abort before COMMIT is sent
   * destroys the connection (the server rolls back and drops any lock wait), so nothing the
   * aborted caller started commits later. Only an abort during COMMIT itself leaves the outcome
   * unknown, which a retry with the same key resolves (C3).
   */
  async transaction<T>(work: (client: SqlClient) => Promise<T>, mode = "", signal?: AbortSignal): Promise<T> {
    const client = await connect(this.pool, signal);
    let released = false;
    const release = (destroy: boolean) => {
      if (released) return;
      released = true;
      client.release(destroy);
    };
    let committing = false;
    const onAbort = () => { if (!committing) release(true); };
    signal?.addEventListener("abort", onAbort, { once: true });
    // An abort between the connection's arrival and here fired before the listener existed.
    if (signal?.aborted) {
      signal.removeEventListener("abort", onAbort);
      release(false);
      signal.throwIfAborted();
    }
    try {
      await client.query(`BEGIN${mode ? ` ${mode}` : ""}`);
      await client.query(`SET LOCAL ROLE ${this.#role}`);
      await client.query(`SET LOCAL lock_timeout = '${LOCK_TIMEOUT}'`);
      await client.query(`SET LOCAL statement_timeout = '${STATEMENT_TIMEOUT}'`);
      const result = await work(client);
      // An abort before this point destroyed the connection (onAbort), so COMMIT cannot run.
      committing = true;
      await client.query("COMMIT");
      return result;
    } catch (error) {
      if (!released) await client.query("ROLLBACK").catch(() => release(true));
      signal?.throwIfAborted();
      throw error;
    } finally {
      signal?.removeEventListener("abort", onAbort);
      release(false);
    }
  }

  async append(principal: RecordsPrincipal, input: unknown, options: RecordsCallOptions = {}): Promise<RecordReceipt> {
    const { signal } = options;
    signal?.throwIfAborted();
    if (!principal.id?.trim()) throw new Error("records.append needs an authenticated caller");
    const args = validateAppend(input, { importer: principal.importer === true, mirror: principal.mirror === true });
    const author = args.author ?? principal.id;
    const authorName = args.author ? null : principal.name ?? null;
    const hash = payloadHash(args);
    // The archive frontier is read before the lock, never inside it (a wal-g run takes seconds).
    const gate = this.options.admission;
    if (gate?.enabled && !gate.refreshed) await gate.refresh(signal);
    const result = await this.transaction(async (client) => {
      // The per-org lock: commit order equals seq, and the key check below cannot race (C3, C4).
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [`fabric-records:${this.org}`]);
      await client.query("SET LOCAL synchronous_commit = on");
      const existing = await client.query<RecordRow>(`SELECT ${RECORD_COLUMNS} FROM records WHERE org = $1 AND author = $2 AND key = $3`, [this.org, author, args.key]);
      const found = existing.rows[0];
      if (found) {
        // An identical retry gets the original receipt; a different payload under the key is refused.
        if (found.payload_hash !== hash) throw new RecordKeyConflictError(args.key, receipt(found));
        return { receipt: receipt(found), committed: false };
      }
      await this.#admit(client);
      const ref = args.ref ?? await this.#allocateRef(client, args.repo!);
      const parsed = parseRef(ref);
      await this.#checkReferences(client, args, ref, author);
      const next = await client.query<{ seq: string }>("SELECT coalesce(max(seq), 0) + 1 AS seq FROM records WHERE origin = $1", [this.origin]);
      const seq = next.rows[0]!.seq;
      const inserted = await client.query<RecordRow>(
        `INSERT INTO records (org, origin, seq, ref, kind, author, author_name, text, data, supersedes, key, payload_hash)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
         RETURNING ${RECORD_COLUMNS}`,
        [this.org, this.origin, seq, ref, args.kind, author, authorName, args.text ?? null, JSON.stringify(args.data ?? {}), args.supersedes ?? null, args.key, hash],
      );
      const row = inserted.rows[0]!;
      await this.#outbox(client, row, args, parsed);
      await client.query(
        "INSERT INTO publication (record_id, origin, seq, topic, recipient) VALUES ($1, $2, $3, $4, $5)",
        [row.id, this.origin, seq, recordTopic(parsed), typeof args.data?.to === "string" ? args.data.to : null],
      );
      return { receipt: receipt(row), committed: true };
    }, "", signal);
    if (result.committed) {
      // The recovery bound, read after COMMIT (C2). A crash before it is filled by the next check.
      await this.transaction((client) => this.fillBounds(client, "recent")).catch(() => undefined);
      this.options.onCommitted?.();
    }
    return result.receipt;
  }

  /**
   * Give committed local records without a recovery bound the insert position now, which is past
   * their commits. `recent` looks only past the newest bounded record (every append and check);
   * `all` also finds a record a crash left unbounded below it (at service start).
   */
  async fillBounds(client: SqlClient, scope: "recent" | "all"): Promise<void> {
    await client.query(
      `INSERT INTO record_bounds (record_id, origin, seq, bound)
       SELECT r.id, r.origin, r.seq, pg_current_wal_insert_lsn() FROM records r
       WHERE r.origin = $1 AND ${scope === "all"
        ? "NOT EXISTS (SELECT 1 FROM record_bounds b WHERE b.record_id = r.id)"
        : "r.seq > coalesce((SELECT max(seq) FROM record_bounds WHERE origin = $1), 0)"}
       ON CONFLICT (record_id) DO NOTHING`,
      [this.origin],
    );
  }

  /** C2: refuse while the off-host recoverable frontier lags the insert position too far. */
  async #admit(client: SqlClient): Promise<void> {
    const gate = this.options.admission;
    if (!gate?.enabled) return;
    const input = await this.admissionInput(client, gate.frontier());
    gate.check(input);
  }

  /**
   * The insert position and the oldest local record the frontier does not cover: one whose
   * recovery bound (through its COMMIT) lies past the frontier. Persisted, so a restart that
   * loses the gate's in-memory samples still sees an old uncovered record.
   */
  async admissionInput(client: SqlClient, frontier: string | undefined): Promise<{ insertLsn: string; oldestUncoveredAt?: number }> {
    await this.fillBounds(client, "recent");
    const { rows } = await client.query<{ insert_lsn: string; oldest: Date | null }>(
      `SELECT pg_current_wal_insert_lsn()::text AS insert_lsn,
        (SELECT min(r.created_at) FROM record_bounds b JOIN records r ON r.id = b.record_id
          WHERE b.origin = $1 AND b.bound > coalesce($2::pg_lsn, '0/0'::pg_lsn)) AS oldest`,
      [this.origin, frontier ?? null],
    );
    const row = rows[0]!;
    return { insertLsn: row.insert_lsn, ...(row.oldest ? { oldestUncoveredAt: row.oldest.getTime() } : {}) };
  }

  /** A new issue's immutable Node-native ref, Owner/repo#L<n> (C11). */
  async #allocateRef(client: SqlClient, repo: string): Promise<string> {
    const { owner, repo: name } = parseRepo(repo);
    const prefix = `${owner}/${name}#L`;
    const { rows } = await client.query<{ n: string }>(
      "SELECT coalesce(max(substring(ref FROM length($1) + 1)::bigint), 0) + 1 AS n FROM records WHERE starts_with(ref, $1)",
      [prefix],
    );
    return `${prefix}${rows[0]!.n}`;
  }

  async #checkReferences(client: SqlClient, args: AppendArgs, ref: string, author: string): Promise<void> {
    if (args.supersedes) {
      const { rows } = await client.query<{ ref: string; kind: string; author: string }>("SELECT ref, kind, author FROM records WHERE id = $1", [args.supersedes]);
      const target = rows[0];
      if (!target) throw new RecordsArgumentError(`supersedes names no record: ${args.supersedes}`);
      if (target.ref !== ref || target.kind !== args.kind) throw new RecordsArgumentError(`supersedes must name a ${args.kind} record on ${ref}`);
      // Replacing is an edit of the author's own record (and of its forge object); never another's.
      if (AUTHOR_OWNED_SUPERSEDE.has(args.kind) && target.author !== author) {
        throw new RecordsArgumentError(`supersedes names a ${args.kind} record by another author; only its author replaces it`);
      }
    }
    const named = args.kind === "answer" ? args.data?.ask : args.kind === "mirror" ? args.data?.mirrorOf : undefined;
    if (typeof named === "string") {
      const { rows } = await client.query<{ ref: string; kind: string }>("SELECT ref, kind FROM records WHERE id = $1", [named.toLowerCase()]);
      const target = rows[0];
      if (!target || target.ref !== ref || (args.kind === "answer" && target.kind !== "ask")) {
        throw new RecordsArgumentError(args.kind === "answer" ? `data.ask must name an ask on ${ref}` : `data.mirrorOf must name a record on ${ref}`);
      }
    }
  }

  /** The mirror's outbox row (§7, Appendix B v1.3), in the record's transaction; mirrored kinds only. */
  async #outbox(client: SqlClient, row: RecordRow, args: AppendArgs, parsed: ReturnType<typeof parseRef>): Promise<void> {
    const mirror = this.options.mirror;
    // An imported record is already on GitHub; mirroring it back would echo.
    if (!mirror?.enabled || !MIRRORED_KINDS.has(args.kind) || args.data?.via !== undefined) return;
    const repo = `${parsed.owner}/${parsed.repo}`;
    let skipped = mirror.repos !== undefined && !mirror.repos.includes(repo);
    const editOf = await this.#editOf(client, row, args);
    const issue = parsed.native ? "{number}" : String(parsed.number);
    const marker = `\n\n<!-- smarty-record:${row.id} -->`;
    let method: string;
    let endpoint: string;
    let body: Record<string, unknown>;
    if (args.kind === "issue") {
      const fields = Object.fromEntries(Object.entries(args.data ?? {}).filter(([name]) => name === "title" || name === "labels"));
      const text = typeof args.data?.body === "string" ? args.data.body : undefined;
      if (args.repo !== undefined) {
        // Creation (a new Node-native ref): the whole issue.
        body = { ...fields, body: `${text ?? args.text ?? ""}${marker}` };
        [method, endpoint] = ["POST", `/repos/${repo}/issues`];
      } else {
        // An update PATCHes only the fields it carries: an absent body stays as it is on the forge.
        body = { ...fields, ...(text !== undefined ? { body: `${text}${marker}` } : {}) };
        [method, endpoint] = ["PATCH", `/repos/${repo}/issues/${issue}`];
        // Nothing the forge shows changed (a stage command): recorded as skipped, never an empty PATCH.
        if (Object.keys(body).length === 0) skipped = true;
      }
    } else if (args.kind === "close" || args.kind === "reopen") {
      body = { state: args.kind === "close" ? "closed" : "open" };
      [method, endpoint] = ["PATCH", `/repos/${repo}/issues/${issue}`];
    } else {
      body = { body: `${args.text ?? ""}${marker}` };
      // The drainer resolves {github_id} from edit_of's record.mirror; it never guesses from text.
      [method, endpoint] = editOf ? ["PATCH", `/repos/${repo}/issues/comments/{github_id}`] : ["POST", `/repos/${repo}/issues/${issue}/comments`];
    }
    await client.query(
      `INSERT INTO outbox (owner, repo, method, endpoint, body, scope, target_thread, who, state, record_id, edit_of, request_key)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
      [parsed.owner, parsed.repo, method, endpoint, JSON.stringify(body), JSON.stringify({ repository: repo, permissions: { issues: "write" } }),
        row.ref, row.author, skipped ? "skipped" : "pending", row.id, editOf ?? null, `record:${row.id}`],
    );
  }

  /**
   * The record whose forge object this one edits (C5): along the supersedes chain, and for a
   * status the author's one status object on the ref.
   */
  async #editOf(client: SqlClient, row: RecordRow, args: AppendArgs): Promise<string | undefined> {
    if (args.supersedes) {
      const { rows } = await client.query<{ root: string }>("SELECT coalesce(edit_of, record_id) AS root FROM outbox WHERE record_id = $1 ORDER BY seq LIMIT 1", [args.supersedes]);
      return rows[0]?.root ?? args.supersedes;
    }
    if (args.kind !== "status") return undefined;
    const { rows } = await client.query<{ root: string }>(
      `SELECT coalesce(o.edit_of, o.record_id) AS root FROM outbox o JOIN records r ON r.id = o.record_id
       WHERE r.ref = $1 AND r.author = $2 AND r.kind = 'status' AND r.id <> $3 ORDER BY o.seq LIMIT 1`,
      [row.ref, row.author, row.id],
    );
    return rows[0]?.root;
  }

  async read(_principal: RecordsPrincipal, input: unknown = {}, options: RecordsCallOptions = {}): Promise<RecordsPage> {
    const args = (input ?? {}) as Record<string, unknown>;
    if (!isObject(args)) throw new RecordsArgumentError("records.read takes {after?, limit?, origin?, ref?, kind?, to?}");
    checkKeys("read", args, ["after", "limit", "origin", "ref", "kind", "to"]);
    const after = optionalInteger("after", args.after, 0, Number.MAX_SAFE_INTEGER) ?? 0;
    const limit = optionalInteger("limit", args.limit, 1, MAX_PAGE) ?? 100;
    const origin = optionalString("origin", args.origin) ?? this.origin;
    const ref = optionalString("ref", args.ref);
    if (ref) parseRef(ref);
    const kind = optionalString("kind", args.kind);
    const to = optionalString("to", args.to);
    return this.page({ after, limit, origin, ...(ref ? { ref } : {}), ...(kind ? { kind: kind as RecordKind } : {}), ...(to ? { to: [to] } : {}) }, options.signal);
  }

  /**
   * Records after a cursor, up to the committed frontier, from one snapshot. Commit order is seq
   * order (the append lock), so no record below the frontier can still appear later.
   */
  async page(args: PageArgs, signal?: AbortSignal): Promise<RecordsPage> {
    return this.transaction(async (client) => {
      const frontier = await client.query<{ seq: string }>("SELECT coalesce(max(seq), 0) AS seq FROM records WHERE origin = $1", [args.origin]);
      const values: unknown[] = [args.origin, args.after, args.limit];
      const where = ["origin = $1", "seq > $2"];
      if (args.ref) { values.push(args.ref); where.push(`ref = $${values.length}`); }
      if (args.kind) { values.push(args.kind); where.push(`kind = $${values.length}`); }
      if (args.to) { values.push([...args.to]); where.push(`data->>'to' = ANY($${values.length}::text[])`); }
      if (args.exceptAuthor) { values.push(args.exceptAuthor); where.push(`author <> $${values.length}`); }
      const { rows } = await client.query<RecordRow>(`SELECT ${RECORD_COLUMNS} FROM records WHERE ${where.join(" AND ")} ORDER BY seq LIMIT $3`, values);
      const records = rows.map(envelope);
      const top = Number(frontier.rows[0]!.seq);
      // A full page ends at its last record; a short page has read everything up to the frontier.
      const next = records.length === args.limit ? records.at(-1)!.sequence : Math.max(args.after, top);
      return { records, next, frontier: top, origin: args.origin };
    }, "ISOLATION LEVEL REPEATABLE READ READ ONLY", signal);
  }

  async openConsumer(consumer: string, names: readonly string[], signal?: AbortSignal): Promise<ConsumerState> {
    return this.transaction(async (client) => {
      const { rows } = await client.query<{ after: string; pending: ConsumerState["pending"] }>(
        "SELECT after, pending FROM consumer_open($1, $2, $3::jsonb)", [consumer, this.origin, JSON.stringify(names)]);
      return { after: Number(rows[0]!.after), pending: rows[0]!.pending };
    }, "", signal);
  }

  async saveConsumer(consumer: string, after: number, pending: ConsumerState["pending"], signal?: AbortSignal): Promise<void> {
    await this.transaction((client) => client.query("SELECT consumer_save($1, $2, $3::jsonb)", [consumer, after, pending ? JSON.stringify(pending) : null]), "", signal);
  }

  async claimPublications(limit: number, signal?: AbortSignal): Promise<ClaimedPublication[]> {
    return this.transaction(async (client) => {
      const { rows } = await client.query<{ record_id: string; seq: string; topic: string; recipient: string | null; kind: string; ref: string; author: string; key: string; text: string | null; created_at: Date }>(
        "SELECT * FROM publication_claim($1, $2, $3)", [this.origin, limit, PUBLICATION_LEASE_SECONDS]);
      return rows.map((row) => ({
        recordId: row.record_id, sequence: Number(row.seq), topic: row.topic, recipient: row.recipient, kind: row.kind, ref: row.ref,
        from: row.author, key: row.key, text: row.text, createdAt: row.created_at.getTime(),
      }));
    }, "", signal);
  }

  async ackPublication(recordId: string, meshSequence: number, signal?: AbortSignal): Promise<void> {
    await this.transaction((client) => client.query("SELECT publication_ack($1, $2)", [recordId, meshSequence]), "", signal);
  }

  async failPublication(recordId: string, error: string, signal?: AbortSignal): Promise<void> {
    await this.transaction((client) => client.query("SELECT publication_fail($1, $2)", [recordId, error]), "", signal);
  }

  async releasePublications(recordIds: readonly string[], signal?: AbortSignal): Promise<void> {
    if (recordIds.length === 0) return;
    await this.transaction((client) => client.query("SELECT publication_release($1::uuid[])", [[...recordIds]]), "", signal);
  }

  async unpublished(signal?: AbortSignal): Promise<number> {
    return this.transaction(async (client) => {
      const { rows } = await client.query<{ n: string }>("SELECT count(*) AS n FROM publication WHERE published_at IS NULL AND origin = $1", [this.origin]);
      return Number(rows[0]!.n);
    }, "", signal);
  }

  /** Consumers with an addressed record past their cursor older than the lag bound (C4). */
  async lagging(lagMs: number, now: number, signal?: AbortSignal): Promise<ConsumerLag[]> {
    return this.transaction(async (client) => {
      const { rows } = await client.query<{ consumer: string; after: string; oldest: Date; n: string }>(
        `SELECT c.consumer, c.after, min(r.created_at) AS oldest, count(*) AS n
         FROM consumers c JOIN records r ON r.origin = c.origin AND r.seq > c.after AND r.author <> c.consumer
           AND r.data->>'to' IN (SELECT jsonb_array_elements_text(c.names))
         WHERE c.origin = $1
         GROUP BY c.consumer, c.after
         HAVING min(r.created_at) <= to_timestamp($2)`,
        [this.origin, (now - lagMs) / 1000],
      );
      return rows.map((row) => ({ consumer: row.consumer, after: Number(row.after), oldestAt: row.oldest.getTime(), count: Number(row.n) }));
    }, "", signal);
  }

  /** One alarm per key per window across every process. */
  async claimAlarm(key: string, now: number, realarmMs: number, signal?: AbortSignal): Promise<boolean> {
    return this.transaction(async (client) => {
      const { rows } = await client.query<{ claimed: boolean }>("SELECT alarm_claim($1, to_timestamp($2), to_timestamp($3)) AS claimed", [key, now / 1000, (now - realarmMs) / 1000]);
      return rows[0]!.claimed;
    }, "", signal);
  }

  /** Records by id, in seq order (a pending inbox batch's replay). */
  async byIds(ids: readonly string[], signal?: AbortSignal): Promise<RecordEnvelope[]> {
    if (ids.length === 0) return [];
    return this.transaction(async (client) => {
      const { rows } = await client.query<RecordRow>(`SELECT ${RECORD_COLUMNS} FROM records WHERE id = ANY($1::uuid[]) ORDER BY seq`, [[...ids]]);
      return rows.map(envelope);
    }, "READ ONLY", signal);
  }

  async get(_principal: RecordsPrincipal, input: unknown, options: RecordsCallOptions = {}): Promise<RecordsGetResult> {
    if (!isObject(input)) throw new RecordsArgumentError("records.get takes {ref, after?, limit?}");
    checkKeys("get", input, ["ref", "after", "limit"]);
    const ref = optionalString("ref", input.ref);
    if (!ref) throw new RecordsArgumentError("records.get needs ref");
    parseRef(ref);
    const after = optionalInteger("after", input.after, 0, Number.MAX_SAFE_INTEGER) ?? 0;
    const limit = optionalInteger("limit", input.limit, 1, MAX_PAGE) ?? 50;
    return this.transaction(async (client) => {
      const issue = await client.query<{ data: Record<string, unknown>; open: boolean }>("SELECT data, open FROM current_issue WHERE ref = $1", [ref]);
      const statuses = await client.query<{ author: string; author_name: string | null; id: string; created_at: Date; text: string | null; data: Record<string, unknown> }>(
        "SELECT author, author_name, id, created_at, text, data FROM current_statuses WHERE ref = $1", [ref]);
      const list = async (view: string) => (await client.query<RecordRow>(`SELECT ${RECORD_COLUMNS} FROM records WHERE id IN (SELECT id FROM ${view} WHERE ref = $1) ORDER BY seq`, [ref])).rows.map(envelope);
      const mirror = await client.query<{ record_id: string; data: Record<string, unknown> }>("SELECT record_id, data FROM mirror_state WHERE ref = $1", [ref]);
      const history = await client.query<RecordRow>(`SELECT ${RECORD_COLUMNS} FROM records WHERE ref = $1 AND seq > $2 ORDER BY seq LIMIT $3`, [ref, after, limit + 1]);
      const fields = issue.rows[0]?.data ?? {};
      const state: RecordFold = {
        ...Object.fromEntries(["title", "body", "owner", "acceptance", "labels", "nextAction", "stage"].filter((name) => fields[name] !== undefined).map((name) => [name, fields[name]])),
        open: issue.rows[0]?.open ?? true,
        statuses: Object.fromEntries(statuses.rows.map((row) => [row.author, {
          id: row.id, at: row.created_at.getTime(), ...(row.text !== null ? { text: row.text } : {}),
          ...(row.author_name ? { name: row.author_name } : {}),
          ...Object.fromEntries(["state", "eta", "waitOn"].filter((name) => row.data[name] !== undefined).map((name) => [name, row.data[name]])),
        }])),
        decisions: await list("current_decisions"),
        openAsks: await list("open_asks"),
        links: await list("current_links"),
        mirror: Object.fromEntries(mirror.rows.map((row) => [row.record_id, row.data])),
      };
      const rows = history.rows.slice(0, limit).map(envelope);
      return { ref, state, history: rows, ...(history.rows.length > limit ? { next: rows.at(-1)!.sequence } : {}) };
    }, "ISOLATION LEVEL REPEATABLE READ READ ONLY", options.signal);
  }

  /** A query for views (the ETA alarm, the board, `smarty log --open`); never a delivery path (C4). */
  async list(_principal: RecordsPrincipal, input: unknown = {}, options: RecordsCallOptions = {}): Promise<RecordsListResult> {
    const args = (input ?? {}) as Record<string, unknown>;
    if (!isObject(args)) throw new RecordsArgumentError("records.list takes one filter object");
    checkKeys("list", args, ["org", "repo", "open", "owner", "hasOpenAsk", "updatedSince", "limit", "after"]);
    const org = optionalString("org", args.org);
    if (org !== undefined && org !== this.org) return { items: [] };
    const repo = optionalString("repo", args.repo);
    if (repo) parseRepo(repo);
    const owner = optionalString("owner", args.owner);
    for (const name of ["open", "hasOpenAsk"] as const) {
      if (args[name] !== undefined && typeof args[name] !== "boolean") throw new RecordsArgumentError(`${name} must be a boolean`);
    }
    const updatedSince = optionalInteger("updatedSince", args.updatedSince, 0, Number.MAX_SAFE_INTEGER);
    const limit = optionalInteger("limit", args.limit, 1, MAX_PAGE) ?? 100;
    const after = optionalString("after", args.after);
    const values: unknown[] = [];
    const where: string[] = [];
    const bind = (value: unknown) => { values.push(value); return `$${values.length}`; };
    if (repo) where.push(`split_part(i.ref, '#', 1) = ${bind(repo)}`);
    if (typeof args.open === "boolean") where.push(`i.open = ${bind(args.open)}`);
    if (owner) where.push(`i.data->>'owner' = ${bind(owner)}`);
    if (typeof args.hasOpenAsk === "boolean") where.push(`${args.hasOpenAsk ? "" : "NOT "}EXISTS (SELECT 1 FROM open_asks a WHERE a.ref = i.ref)`);
    if (updatedSince !== undefined) where.push(`i.updated_at >= to_timestamp(${bind(updatedSince / 1000)})`);
    if (after) {
      let cursor: { u?: unknown; r?: unknown };
      try { cursor = JSON.parse(Buffer.from(after, "base64url").toString("utf8")) as { u?: unknown; r?: unknown }; } catch { cursor = {}; }
      if (typeof cursor.u !== "string" || !/^\d+$/.test(cursor.u) || typeof cursor.r !== "string") throw new RecordsArgumentError("after must be the previous page's next");
      where.push(`(i.updated_us, i.ref) < (${bind(cursor.u)}::bigint, ${bind(cursor.r)})`);
    }
    const sql = `
      SELECT * FROM (
        SELECT i.ref, i.data, i.open, i.updated_at, (extract(epoch FROM i.updated_at) * 1000000)::bigint AS updated_us,
          coalesce((SELECT jsonb_object_agg(s.author, jsonb_strip_nulls(jsonb_build_object('at', (extract(epoch FROM s.created_at) * 1000)::bigint, 'state', s.data->'state', 'eta', s.data->'eta')))
            FROM current_statuses s WHERE s.ref = i.ref), '{}'::jsonb) AS statuses,
          coalesce((SELECT jsonb_agg(jsonb_strip_nulls(jsonb_build_object('id', a.id, 'to', a.data->>'to', 'at', (extract(epoch FROM a.created_at) * 1000)::bigint)) ORDER BY a.seq)
            FROM open_asks a WHERE a.ref = i.ref), '[]'::jsonb) AS open_asks
        FROM current_issue i
      ) i ${where.length ? `WHERE ${where.join(" AND ")}` : ""}
      ORDER BY i.updated_us DESC, i.ref DESC LIMIT ${bind(limit + 1)}`;
    return this.transaction(async (client) => {
      const { rows } = await client.query<{ ref: string; data: Record<string, unknown>; open: boolean; updated_at: Date; updated_us: string; statuses: RecordsListItem["statuses"]; open_asks: RecordsListItem["openAsks"] }>(sql, values);
      const items = rows.slice(0, limit).map((row) => ({
        ref: row.ref,
        ...Object.fromEntries(["title", "owner", "stage"].filter((name) => typeof row.data[name] === "string").map((name) => [name, row.data[name]])),
        open: row.open, updatedAt: row.updated_at.getTime(), statuses: row.statuses, openAsks: row.open_asks,
      }) as RecordsListItem);
      const last = rows[limit - 1];
      return { items, ...(rows.length > limit && last ? { next: Buffer.from(JSON.stringify({ u: String(last.updated_us), r: last.ref })).toString("base64url") } : {}) };
    }, "ISOLATION LEVEL REPEATABLE READ READ ONLY", options.signal);
  }

  async close(): Promise<void> { await this.pool.end?.(); }
}
