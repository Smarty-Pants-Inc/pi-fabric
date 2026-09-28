/**
 * The record store's schema in the org's own PostgreSQL database (smarty-dev#754 §1-2, C3, C4,
 * C13). Migrations are applied in order under a transaction-scoped advisory lock and recorded in
 * `records_schema`, so concurrent Fabric processes apply each one once.
 *
 * Authority (C10, C13): the migration role (the cluster's `postgres`, used only at install) owns
 * every object. The records service logs in as `records_service`, a member of
 * `fabric_records_writer`, and every transaction also runs SET LOCAL ROLE to it. That role has
 * INSERT and SELECT on the tables, EXECUTE on the SECURITY DEFINER functions that change the
 * mutable state (cursors, publication acks, claims), and nothing else: no UPDATE, DELETE,
 * TRUNCATE or DDL. A trigger also refuses UPDATE, DELETE and TRUNCATE of records for every role.
 */

export const WRITER_ROLE = "fabric_records_writer";
/** The service's login role; install creates it, the migration grants it the writer role. */
export const SERVICE_ROLE = "records_service";

const roles = `
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${WRITER_ROLE}') THEN CREATE ROLE ${WRITER_ROLE} NOLOGIN; END IF;
END $$;`;

const v1 = `
${roles}

CREATE TABLE records (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  org text NOT NULL,
  origin text NOT NULL,
  seq bigint NOT NULL CHECK (seq > 0),
  ref text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('issue','status','comment','decision','ask','answer','handoff','link','close','reopen','mirror')),
  author text NOT NULL,
  author_name text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  text text,
  data jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(data) = 'object')
    -- Record ids in data are stored in one (lowercase) form, whoever writes them.
    CHECK (NOT data ? 'ask' OR data->>'ask' = lower(data->>'ask'))
    CHECK (NOT data ? 'mirrorOf' OR data->>'mirrorOf' = lower(data->>'mirrorOf')),
  supersedes uuid REFERENCES records(id),
  key text NOT NULL,
  payload_hash text NOT NULL,
  UNIQUE (origin, seq),
  UNIQUE (org, author, key)
);
CREATE INDEX records_ref_seq ON records (ref, seq);
CREATE INDEX records_kind_ref ON records (kind, ref, seq);
CREATE INDEX records_supersedes ON records (supersedes) WHERE supersedes IS NOT NULL;
CREATE INDEX records_to ON records ((data->>'to'), seq) WHERE data ? 'to';

CREATE FUNCTION records_append_only() RETURNS trigger LANGUAGE plpgsql AS $f$
BEGIN
  RAISE EXCEPTION 'records are append-only: % refused', TG_OP USING ERRCODE = 'insufficient_privilege';
END $f$;
CREATE TRIGGER records_no_update_delete BEFORE UPDATE OR DELETE ON records
  FOR EACH ROW EXECUTE FUNCTION records_append_only();
CREATE TRIGGER records_no_truncate BEFORE TRUNCATE ON records
  FOR EACH STATEMENT EXECUTE FUNCTION records_append_only();

-- C2: an upper bound on the WAL a record's recovery needs, through its COMMIT: the insert
-- position read after the commit (by the appender, or by any later reader, since a committed
-- record's commit precedes every later insert position). Never the pre-insert position.
CREATE TABLE record_bounds (
  record_id uuid PRIMARY KEY REFERENCES records(id),
  origin text NOT NULL,
  seq bigint NOT NULL,
  bound pg_lsn NOT NULL
);
CREATE INDEX record_bounds_origin_bound ON record_bounds (origin, bound);
CREATE INDEX record_bounds_origin_seq ON record_bounds (origin, seq);

-- dev-lead's #1481 outbox row with the v1.3 additions (record_id, edit_of, request_key, skipped).
CREATE TABLE outbox (
  seq bigserial PRIMARY KEY,
  owner text NOT NULL,
  repo text NOT NULL,
  method text NOT NULL CHECK (method IN ('POST','PATCH','PUT','DELETE')),
  endpoint text NOT NULL,
  body jsonb,
  scope jsonb,
  target_thread text NOT NULL,
  who text NOT NULL,
  enqueued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','posted','refused','skipped','unknown')),
  attempts integer NOT NULL DEFAULT 0,
  posted_url text,
  github_id text,
  error text,
  record_id uuid REFERENCES records(id),
  edit_of uuid REFERENCES records(id),
  request_key text NOT NULL,
  UNIQUE (record_id, owner, repo, target_thread),
  UNIQUE (owner, request_key)
);
CREATE INDEX outbox_pending ON outbox (target_thread, seq) WHERE state = 'pending';

-- The mesh nudge for each record, written in the record's transaction (C4, "commit, then nudge").
CREATE TABLE publication (
  record_id uuid PRIMARY KEY REFERENCES records(id),
  origin text NOT NULL,
  seq bigint NOT NULL,
  topic text NOT NULL,
  recipient text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  published_at timestamptz,
  mesh_sequence bigint,
  attempts integer NOT NULL DEFAULT 0,
  error text,
  claimed_at timestamptz,
  -- Who holds the claim: the relay's principal and a random claim id. Only that claim may ack,
  -- fail or release the row, and only while its lease lasts.
  claimed_by text,
  claim_id uuid
);
CREATE INDEX publication_unpublished ON publication (origin, seq) WHERE published_at IS NULL;

-- Each consumer's processing cursor (C4): advanced only after it acted; read by the idle watchdog.
CREATE TABLE consumers (
  consumer text PRIMARY KEY,
  origin text NOT NULL,
  after bigint NOT NULL,
  names jsonb NOT NULL DEFAULT '[]'::jsonb,
  pending jsonb,
  advanced_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  seen_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- One alarm per key per window across every process (consumer lag, archive lag).
CREATE TABLE alarms (
  key text PRIMARY KEY,
  raised_at timestamptz NOT NULL
);

-- C10: the service's principals. A session or actor registers its own participant id once
-- (the first claim wins; a second is refused); ids outside that shape are issued by the
-- operator. Only a token's hash is stored. Roles (importer, mirror) live in the service's
-- own configuration, never here and never in a caller's.
CREATE TABLE principals (
  id text PRIMARY KEY,
  name text,
  token_hash text NOT NULL UNIQUE,
  issued_by text NOT NULL CHECK (issued_by IN ('register', 'operator')),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- C2: one process per refresh interval runs each archive check; every process reads the result.
CREATE TABLE archive_checks (
  target text PRIMARY KEY,
  frontier pg_lsn,
  checked_at timestamptz,
  error text,
  claimed_at timestamptz
);

-- Fold views (§2). A record another record supersedes is out of every list fold.
CREATE VIEW live_records AS
  SELECT r.* FROM records r WHERE NOT EXISTS (SELECT 1 FROM records s WHERE s.supersedes = r.id);

-- The current issue: each field's newest value over the ref's issue records, so a stage
-- command that carries only stage keeps the title. Open unless the newest close/reopen closed it.
CREATE VIEW current_issue AS
  SELECT refs.ref,
    coalesce(fields.data, '{}'::jsonb) AS data,
    coalesce(state.kind, 'reopen') <> 'close' AS open,
    refs.updated_at, refs.seq
  FROM (SELECT ref, max(created_at) AS updated_at, max(seq) AS seq FROM records GROUP BY ref) refs
  LEFT JOIN (
    SELECT ref, jsonb_object_agg(field, value) AS data FROM (
      SELECT DISTINCT ON (r.ref, f.key) r.ref, f.key AS field, f.value
      FROM records r CROSS JOIN LATERAL jsonb_each(r.data) f
      WHERE r.kind = 'issue' AND f.key NOT IN ('via', 'githubId')
      ORDER BY r.ref, f.key, r.seq DESC
    ) newest GROUP BY ref
  ) fields ON fields.ref = refs.ref
  LEFT JOIN LATERAL (
    SELECT kind FROM records c WHERE c.ref = refs.ref AND c.kind IN ('close','reopen') ORDER BY c.seq DESC LIMIT 1
  ) state ON true;

-- Each author's current status on a ref: the newest record.status by that author.
CREATE VIEW current_statuses AS
  SELECT DISTINCT ON (ref, author) ref, author, author_name, id, seq, created_at, text, data
  FROM records WHERE kind = 'status' ORDER BY ref, author, seq DESC;

-- An ask is open until a record.answer names it.
CREATE VIEW open_asks AS
  SELECT a.ref, a.id, a.seq, a.author, a.created_at, a.text, a.data
  FROM live_records a
  WHERE a.kind = 'ask'
    AND NOT EXISTS (SELECT 1 FROM records b WHERE b.kind = 'answer' AND b.data->>'ask' = a.id::text);

CREATE VIEW current_links AS
  SELECT ref, id, seq, author, created_at, text, data FROM live_records WHERE kind = 'link';

CREATE VIEW current_decisions AS
  SELECT ref, id, seq, author, created_at, text, data FROM live_records WHERE kind = 'decision';

-- "Is X on the forge": the newest record.mirror for mirrorOf X.
CREATE VIEW mirror_state AS
  SELECT DISTINCT ON (data->>'mirrorOf') (data->>'mirrorOf')::uuid AS record_id, ref, id, seq, created_at, data
  FROM records WHERE kind = 'mirror' ORDER BY data->>'mirrorOf', seq DESC;

-- The mutable state changes only through these functions (the writer has EXECUTE, not UPDATE).
CREATE FUNCTION consumer_open(p_consumer text, p_origin text, p_names jsonb)
  RETURNS TABLE (after bigint, pending jsonb) LANGUAGE sql SECURITY DEFINER SET search_path = public, pg_temp AS $f$
  -- A consumer with no cursor starts at the present: the inbox is for what it misses from now on.
  INSERT INTO consumers (consumer, origin, after, names)
    SELECT p_consumer, p_origin, coalesce(max(seq), 0), p_names FROM records WHERE origin = p_origin
    ON CONFLICT (consumer) DO UPDATE SET names = EXCLUDED.names, seen_at = clock_timestamp();
  SELECT c.after, c.pending FROM consumers c WHERE c.consumer = p_consumer;
$f$;
CREATE FUNCTION consumer_save(p_consumer text, p_after bigint, p_pending jsonb)
  RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path = public, pg_temp AS $f$
  UPDATE consumers SET after = greatest(after, p_after), pending = p_pending,
    advanced_at = CASE WHEN p_after > after THEN clock_timestamp() ELSE advanced_at END
  WHERE consumer = p_consumer;
$f$;
CREATE FUNCTION alarm_claim(p_key text, p_at timestamptz, p_before timestamptz)
  RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $f$
BEGIN
  INSERT INTO alarms (key, raised_at) VALUES (p_key, p_at)
    ON CONFLICT (key) DO UPDATE SET raised_at = EXCLUDED.raised_at WHERE alarms.raised_at <= p_before;
  RETURN FOUND;
END $f$;
-- Unpublished nudges for one relay, in seq order; a claim expires, so a relay that died is replaced.
CREATE FUNCTION publication_claim(p_origin text, p_limit integer, p_lease_seconds double precision, p_claimant text)
  RETURNS TABLE (claim_id uuid, record_id uuid, seq bigint, topic text, recipient text, kind text, ref text, author text, key text, text text, created_at timestamptz)
  LANGUAGE sql SECURITY DEFINER SET search_path = public, pg_temp AS $f$
  WITH claim AS (SELECT gen_random_uuid() AS id),
  claimed AS (
    UPDATE publication p SET claimed_at = clock_timestamp(), claimed_by = p_claimant, claim_id = (SELECT id FROM claim)
    WHERE p.record_id IN (
      SELECT q.record_id FROM publication q
      WHERE q.published_at IS NULL AND q.origin = p_origin
        AND (q.claimed_at IS NULL OR q.claimed_at <= clock_timestamp() - make_interval(secs => p_lease_seconds))
      ORDER BY q.seq LIMIT p_limit FOR UPDATE SKIP LOCKED)
    RETURNING p.claim_id, p.record_id, p.seq, p.topic, p.recipient)
  SELECT c.claim_id, c.record_id, c.seq, c.topic, c.recipient, r.kind, r.ref, r.author, r.key, r.text, r.created_at
  FROM claimed c JOIN records r ON r.id = c.record_id ORDER BY c.seq;
$f$;
-- Each completes only the caller's own live claim on the row; anything else changes nothing.
CREATE FUNCTION publication_ack(p_record uuid, p_claim uuid, p_claimant text, p_mesh_sequence bigint, p_lease_seconds double precision)
  RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $f$
BEGIN
  UPDATE publication SET published_at = clock_timestamp(), mesh_sequence = p_mesh_sequence, attempts = attempts + 1, error = NULL,
    claimed_at = NULL, claimed_by = NULL, claim_id = NULL
  WHERE record_id = p_record AND published_at IS NULL AND claim_id = p_claim AND claimed_by = p_claimant
    AND claimed_at > clock_timestamp() - make_interval(secs => p_lease_seconds);
  RETURN FOUND;
END $f$;
CREATE FUNCTION publication_fail(p_record uuid, p_claim uuid, p_claimant text, p_error text)
  RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $f$
BEGIN
  UPDATE publication SET attempts = attempts + 1, error = left(p_error, 500), claimed_at = NULL, claimed_by = NULL, claim_id = NULL
  WHERE record_id = p_record AND published_at IS NULL AND claim_id = p_claim AND claimed_by = p_claimant;
  RETURN FOUND;
END $f$;
-- A relay that stopped at a failure gives back the rest of its claim at once.
CREATE FUNCTION publication_release(p_records uuid[], p_claim uuid, p_claimant text)
  RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path = public, pg_temp AS $f$
  UPDATE publication SET claimed_at = NULL, claimed_by = NULL, claim_id = NULL
  WHERE record_id = ANY(p_records) AND published_at IS NULL AND claim_id = p_claim AND claimed_by = p_claimant;
$f$;
CREATE FUNCTION archive_claim(p_target text, p_window_seconds double precision)
  RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $f$
BEGIN
  INSERT INTO archive_checks (target) VALUES (p_target) ON CONFLICT (target) DO NOTHING;
  UPDATE archive_checks SET claimed_at = clock_timestamp()
    WHERE target = p_target AND (claimed_at IS NULL OR claimed_at <= clock_timestamp() - make_interval(secs => p_window_seconds));
  RETURN FOUND;
END $f$;
-- A failed check keeps the last good frontier and records why.
CREATE FUNCTION archive_record(p_target text, p_frontier pg_lsn, p_error text)
  RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path = public, pg_temp AS $f$
  UPDATE archive_checks SET frontier = coalesce(p_frontier, frontier),
    checked_at = CASE WHEN p_frontier IS NULL THEN checked_at ELSE clock_timestamp() END, error = left(p_error, 500)
  WHERE target = p_target;
$f$;

REVOKE ALL ON records, record_bounds, outbox, publication, consumers, archive_checks, alarms, principals FROM PUBLIC;
REVOKE ALL ON FUNCTION consumer_open(text, text, jsonb), consumer_save(text, bigint, jsonb), alarm_claim(text, timestamptz, timestamptz),
  publication_claim(text, integer, double precision, text), publication_ack(uuid, uuid, text, bigint, double precision), publication_fail(uuid, uuid, text, text), publication_release(uuid[], uuid, text),
  archive_claim(text, double precision), archive_record(text, pg_lsn, text), records_append_only() FROM PUBLIC;
GRANT SELECT, INSERT ON records, record_bounds, outbox, publication, principals TO ${WRITER_ROLE};
GRANT SELECT ON consumers, archive_checks, alarms TO ${WRITER_ROLE};
GRANT USAGE ON SEQUENCE outbox_seq_seq TO ${WRITER_ROLE};
GRANT SELECT ON live_records, current_issue, current_statuses, open_asks, current_links, current_decisions, mirror_state TO ${WRITER_ROLE};
GRANT EXECUTE ON FUNCTION consumer_open(text, text, jsonb), consumer_save(text, bigint, jsonb), alarm_claim(text, timestamptz, timestamptz),
  publication_claim(text, integer, double precision, text), publication_ack(uuid, uuid, text, bigint, double precision), publication_fail(uuid, uuid, text, text), publication_release(uuid[], uuid, text),
  archive_claim(text, double precision), archive_record(text, pg_lsn, text) TO ${WRITER_ROLE};
`;

export const MIGRATIONS: readonly string[] = [v1];

/** A minimal client: pg's PoolClient satisfies it, and so does a test double. */
export interface SqlClient {
  query<T = Record<string, unknown>>(text: string, values?: unknown[]): Promise<{ rows: T[]; rowCount: number | null }>;
}

/** Apply pending migrations; returns the schema version. Needs a role that may create tables and roles. */
export const migrate = async (client: SqlClient): Promise<number> => {
  await client.query("BEGIN");
  try {
    await client.query("SELECT pg_advisory_xact_lock(hashtextextended('fabric-records:migrate', 0))");
    await client.query("CREATE TABLE IF NOT EXISTS records_schema (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())");
    await client.query("REVOKE ALL ON records_schema FROM PUBLIC");
    await client.query(`GRANT SELECT ON records_schema TO PUBLIC`);
    const { rows } = await client.query<{ version: number | null }>("SELECT max(version) AS version FROM records_schema");
    const current = rows[0]?.version ?? 0;
    for (let version = current + 1; version <= MIGRATIONS.length; version++) {
      await client.query(MIGRATIONS[version - 1]!);
      await client.query("INSERT INTO records_schema (version) VALUES ($1)", [version]);
    }
    // The service's login role (created by install, never by the service) gets the writer role.
    await client.query(`DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${SERVICE_ROLE}') THEN GRANT ${WRITER_ROLE} TO ${SERVICE_ROLE}; END IF;
    END $$`);
    await client.query("COMMIT");
    return MIGRATIONS.length;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
};
