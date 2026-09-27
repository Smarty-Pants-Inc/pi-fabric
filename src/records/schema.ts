/**
 * The record store's schema in the org's own PostgreSQL database (smarty-dev#754 §1-2, C3, C4,
 * C13). Migrations are applied in order under a transaction-scoped advisory lock and recorded in
 * `records_schema`, so concurrent Fabric processes apply each one once.
 *
 * Grants: the records service works as `fabric_records_writer` (SET LOCAL ROLE in every
 * transaction). That role may INSERT and SELECT `records`, never UPDATE, DELETE or TRUNCATE it;
 * a trigger refuses those for every role, the owner included. `outbox` belongs to the mirror
 * (dev-lead's drainer updates its state), so `fabric_records_mirror` may UPDATE it.
 */

export const WRITER_ROLE = "fabric_records_writer";
export const MIRROR_ROLE = "fabric_records_mirror";

const roles = `
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${WRITER_ROLE}') THEN CREATE ROLE ${WRITER_ROLE} NOLOGIN; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${MIRROR_ROLE}') THEN CREATE ROLE ${MIRROR_ROLE} NOLOGIN; END IF;
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
  data jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(data) = 'object'),
  supersedes uuid REFERENCES records(id),
  key text NOT NULL,
  payload_hash text NOT NULL,
  -- The local WAL insert position when the record was written (C2 admission); not replicated meaning.
  origin_lsn pg_lsn,
  UNIQUE (origin, seq),
  UNIQUE (org, author, key)
);
CREATE INDEX records_ref_seq ON records (ref, seq);
CREATE INDEX records_kind_ref ON records (kind, ref, seq);
CREATE INDEX records_supersedes ON records (supersedes) WHERE supersedes IS NOT NULL;
CREATE INDEX records_to ON records ((data->>'to'), seq) WHERE data ? 'to';
CREATE INDEX records_origin_lsn ON records (origin_lsn);

CREATE FUNCTION records_append_only() RETURNS trigger LANGUAGE plpgsql AS $f$
BEGIN
  RAISE EXCEPTION 'records are append-only: % refused', TG_OP USING ERRCODE = 'insufficient_privilege';
END $f$;
CREATE TRIGGER records_no_update_delete BEFORE UPDATE OR DELETE ON records
  FOR EACH ROW EXECUTE FUNCTION records_append_only();
CREATE TRIGGER records_no_truncate BEFORE TRUNCATE ON records
  FOR EACH STATEMENT EXECUTE FUNCTION records_append_only();

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
  error text
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

REVOKE ALL ON records, outbox, publication, consumers FROM PUBLIC;
GRANT SELECT, INSERT ON records TO ${WRITER_ROLE};
GRANT SELECT, INSERT ON outbox TO ${WRITER_ROLE};
GRANT USAGE ON SEQUENCE outbox_seq_seq TO ${WRITER_ROLE};
GRANT SELECT, INSERT, UPDATE ON publication TO ${WRITER_ROLE};
GRANT SELECT, INSERT, UPDATE ON consumers TO ${WRITER_ROLE};
GRANT SELECT ON live_records, current_issue, current_statuses, open_asks, current_links, current_decisions, mirror_state TO ${WRITER_ROLE};
GRANT SELECT, INSERT ON records TO ${MIRROR_ROLE};
GRANT SELECT, INSERT, UPDATE ON outbox TO ${MIRROR_ROLE};
GRANT USAGE ON SEQUENCE outbox_seq_seq TO ${MIRROR_ROLE};
GRANT SELECT ON live_records, current_issue, current_statuses, open_asks, current_links, current_decisions, mirror_state TO ${MIRROR_ROLE};
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
    await client.query("COMMIT");
    return MIGRATIONS.length;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
};
