import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { CHAIN_SELECT, rowHash, type ChainRow } from "../src/records/chain.js";
import { MIGRATIONS, migrate } from "../src/records/schema.js";
import { jsonPrefix, RecordStore, type ClientPool, type RecordsPrincipal } from "../src/records/store.js";
import { postgresBin, startPostgres, type TestPostgres } from "./helpers/postgres.js";

/**
 * The hash chain and its anchors (smarty-dev#754 R3), against a real PostgreSQL. Each tamper is
 * done as the cluster superuser, the way an attacker with database authority would: triggers
 * and foreign keys off (session_replication_role = replica), then a plain UPDATE or DELETE.
 */
const alice: RecordsPrincipal = { id: "session:alice", name: "alice" };
const REF = "Smarty-Pants-Inc/smarty-dev#754";

let server: TestPostgres;
let databases = 0;
const open: pg.Pool[] = [];

const database = async (): Promise<pg.Pool> => {
  const name = `chain_${++databases}`;
  const root = server.pool({ max: 1 });
  await root.query(`CREATE DATABASE ${name}`);
  await root.end();
  const pool = new pg.Pool({ ...server.connection, database: name, max: 4 });
  pool.on("error", () => undefined);
  open.push(pool);
  return pool;
};

const chain = async (count: number) => {
  const pool = await database();
  const client = await pool.connect();
  try { await migrate(client); } finally { client.release(); }
  const store = new RecordStore(pool as unknown as ClientPool, { org: "smarty-pants", origin: "dev1" });
  for (let n = 1; n <= count; n++) await store.append(alice, { ref: REF, kind: "status", key: `k${n}`, text: `record ${n}`, data: { state: "in progress" } });
  return { pool, store };
};

/** As the superuser: no trigger, no foreign key (what the append-only trigger cannot stop). */
const tamper = async (pool: pg.Pool, sql: string, values: unknown[] = []): Promise<void> => {
  const client = await pool.connect();
  try {
    expect((await client.query<{ rolsuper: boolean }>("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")).rows[0]!.rolsuper).toBe(true);
    await client.query("SET session_replication_role = replica");
    const result = await client.query(sql, values);
    expect(result.rowCount).toBeGreaterThan(0);
  } finally {
    await client.query("RESET session_replication_role");
    client.release();
  }
};

describe.skipIf(!postgresBin)("the records hash chain on a real PostgreSQL", () => {
  beforeAll(async () => { server = await startPostgres(); }, 60_000);
  afterAll(async () => {
    await Promise.allSettled(open.map((pool) => pool.end()));
    await server?.stop();
  }, 30_000);

  it("an untouched chain verifies clean; each prev_hash is the previous row's canonical hash", async () => {
    const { pool, store } = await chain(5);
    const anchor = await store.anchor(alice, {});
    expect(anchor).toMatchObject({ org: "smarty-pants", seq: 5, hash: expect.stringMatching(/^[0-9a-f]{64}$/) });
    const result = await store.verify(alice, { anchors: [anchor] });
    expect(result).toMatchObject({ ok: true, clean: true, rows: 5, anchors: { checked: 1, passed: 1, failed: 0 }, failedAnchors: [] });
    expect(result.summary).toBe("clean: 5 records, anchored through seq 5");
    // Independently recomputed from the stored rows.
    const { rows } = await pool.query<ChainRow>(`SELECT ${CHAIN_SELECT} FROM records ORDER BY records.seq`);
    expect(rows[0]!.prev_hash).toBeNull();
    for (let i = 1; i < rows.length; i++) expect(rows[i]!.prev_hash).toBe(rowHash(rows[i - 1]!));
    expect(anchor.hash).toBe(rowHash(rows.at(-1)!));
  });

  it("catches a mid-chain UPDATE, even one JavaScript's Date would round away (1 µs)", async () => {
    for (const edit of ["text = 'forged'", "data = '{\"state\": \"done\"}'::jsonb", "created_at = created_at + interval '1 microsecond'"]) {
      const { pool, store } = await chain(5);
      const anchor = await store.anchor(alice, {});
      const before = (await pool.query<ChainRow>(`SELECT ${CHAIN_SELECT} FROM records WHERE seq = 3`)).rows[0]!;
      await tamper(pool, `UPDATE records SET ${edit} WHERE seq = 3`);
      const after = (await pool.query<ChainRow>(`SELECT ${CHAIN_SELECT} FROM records WHERE seq = 3`)).rows[0]!;
      const result = await store.verify(alice, { anchors: [anchor] });
      expect(result.ok, edit).toBe(false);
      expect(result.clean).toBe(false);
      expect(result.break).toEqual({ org: "smarty-pants", seq: 4, reason: "prev_hash", expected: rowHash(after), found: rowHash(before) });
      expect(result.summary).toContain(`chain broken at smarty-pants seq 4 (prev_hash): expected ${rowHash(after)}, found ${rowHash(before)}`);
    }
  });

  it("catches an era-only edit of an anchored row (AD to BC) and a non-finite created_at (#118 S1)", async () => {
    const { pool, store } = await chain(5);
    const anchor = await store.anchor(alice, {});
    const plain = "to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')";
    const before = (await pool.query<{ t: string }>(`SELECT ${plain} AS t FROM records WHERE seq = 5`)).rows[0]!.t;
    await tamper(pool, "UPDATE records SET created_at = ((created_at AT TIME ZONE 'UTC')::text || ' BC')::timestamp AT TIME ZONE 'UTC' WHERE seq = 5");
    // Only the era changed: the year, month, day and time read the same without it.
    expect((await pool.query<{ t: string; bc: boolean }>(`SELECT ${plain} AS t, to_char(created_at, 'BC') = 'BC' AS bc FROM records WHERE seq = 5`)).rows[0]).toEqual({ t: before, bc: true });
    expect(await store.verify(alice, { anchors: [anchor] })).toMatchObject({ ok: false, clean: false, anchors: { failed: 1 }, failedAnchors: [{ seq: 5 }] });
    for (const value of ["infinity", "-infinity"]) {
      const fresh = await chain(3);
      const a3 = await fresh.store.anchor(alice, {});
      await tamper(fresh.pool, `UPDATE records SET created_at = '${value}' WHERE seq = 2`);
      expect(await fresh.store.verify(alice, { anchors: [a3] }), value).toMatchObject({ ok: false, break: { seq: 3, reason: "prev_hash" } });
    }
  });

  it("catches a mid-chain DELETE", async () => {
    const { pool, store } = await chain(5);
    const anchor = await store.anchor(alice, {});
    await tamper(pool, "DELETE FROM records WHERE seq = 3");
    const result = await store.verify(alice, { anchors: [anchor] });
    expect(result).toMatchObject({ ok: false, clean: false, rows: 4, break: { org: "smarty-pants", seq: 4, reason: "gap", expected: "3", found: "4" } });
  });

  it("catches a mid-chain DELETE whose successors were re-chained and renumbered: the anchor fails", async () => {
    const { pool, store } = await chain(5);
    const anchor = await store.anchor(alice, {});
    await tamper(pool, "DELETE FROM records WHERE seq = 3");
    await tamper(pool, "UPDATE records SET seq = seq - 1 WHERE seq > 3");
    // The attacker rewrites every later prev_hash so the chain itself is consistent again.
    const rows = (await pool.query<ChainRow>(`SELECT ${CHAIN_SELECT} FROM records ORDER BY records.seq`)).rows;
    for (let i = 1; i < rows.length; i++) {
      rows[i]!.prev_hash = rowHash(rows[i - 1]!);
      await tamper(pool, "UPDATE records SET prev_hash = $1 WHERE id = $2", [rows[i]!.prev_hash, rows[i]!.id]);
    }
    const result = await store.verify(alice, { anchors: [anchor] });
    expect(result.break).toBeUndefined();
    expect(result).toMatchObject({ ok: false, clean: false, anchors: { failed: 1 }, failedAnchors: [{ seq: 5, found: null }] });
    expect(result.summary).toContain("anchor smarty-pants seq 5 fails");
  });

  it("catches an edit of the last anchored row", async () => {
    const { pool, store } = await chain(5);
    const anchor = await store.anchor(alice, {});
    await tamper(pool, "UPDATE records SET text = 'forged' WHERE seq = 5");
    const result = await store.verify(alice, { anchors: [anchor] });
    expect(result.break).toBeUndefined();
    expect(result).toMatchObject({ ok: false, clean: false, anchors: { failed: 1 }, failedAnchors: [{ seq: 5, hash: anchor.hash, found: expect.stringMatching(/^[0-9a-f]{64}$/) }] });
    expect(result.failedAnchors[0]!.found).not.toBe(anchor.hash);
  });

  it("catches a deletion of the anchored tail", async () => {
    const { pool, store } = await chain(5);
    const older = await store.anchor(alice, {});
    await store.append(alice, { ref: REF, kind: "comment", key: "k6", text: "record 6" });
    const latest = await store.anchor(alice, {});
    await tamper(pool, "DELETE FROM records WHERE seq >= 5");
    const result = await store.verify(alice, { anchors: [older, latest] });
    expect(result.break).toBeUndefined();
    expect(result).toMatchObject({ ok: false, clean: false, rows: 4, anchors: { checked: 2, passed: 0, failed: 2 }, failedAnchors: [{ seq: 5, found: null }, { seq: 6, found: null }] });
    expect(result.summary).toContain("anchor smarty-pants seq 5 fails: expected");
    expect(result.summary).toContain("(2 of 2 anchors fail)");
  });

  it("reports an edit after the latest anchor as unanchored, not clean", async () => {
    const { pool, store } = await chain(3);
    const anchor = await store.anchor(alice, {});
    await store.append(alice, { ref: REF, kind: "comment", key: "k4", text: "record 4" });
    await store.append(alice, { ref: REF, kind: "comment", key: "k5", text: "record 5" });
    await tamper(pool, "UPDATE records SET text = 'forged' WHERE seq = 5");
    const result = await store.verify(alice, { anchors: [anchor] });
    expect(result).toMatchObject({ ok: true, clean: false, unanchored: { from: 4, to: 5 }, anchors: { passed: 1, failed: 0 } });
    expect(result.summary).toBe("unanchored: seq 4..5");
    // With no anchor at all, nothing is vouched for.
    expect(await store.verify(alice, {})).toMatchObject({ ok: true, clean: false, unanchored: { from: 1, to: 5 } });
  });

  it("chains concurrent appends in commit order", async () => {
    const { store } = await chain(0);
    await Promise.all(Array.from({ length: 30 }, (_, n) => store.append(alice, { ref: REF, kind: "comment", key: `c${n}`, text: `${n}` })));
    expect(await store.verify(alice, { anchors: [await store.anchor(alice, {})] })).toMatchObject({ ok: true, clean: true, rows: 30 });
    expect(await store.anchor(alice, {})).toMatchObject({ seq: 30 });
  });

  it("an empty org anchors at seq 0 and verifies clean", async () => {
    const { store } = await chain(0);
    expect(await store.anchor(alice, {})).toMatchObject({ seq: 0, hash: null });
    const empty = await store.anchor(alice, {});
    expect(await store.verify(alice, {})).toMatchObject({ ok: true, clean: true, rows: 0 });
    // The anchor it just gave is valid input (#118 F1).
    expect(await store.verify(alice, { anchors: [empty] })).toMatchObject({ ok: true, clean: true, rows: 0 });
  });

  it("refuses malformed anchors", async () => {
    const { store } = await chain(1);
    for (const anchors of [[{ seq: 0, hash: "a".repeat(64) }], [{ seq: 1, hash: "XYZ" }], "no", [{ seq: 1 }], [{ seq: 1, hash: null }], [{ seq: 0 }]]) {
      await expect(store.verify(alice, { anchors })).rejects.toThrow(/anchor/);
    }
    await expect(store.verify(alice, { extra: 1 })).rejects.toThrow(/unknown field/);
  });

  it("records.get cuts an escape-heavy fold field to 16 KiB of JSON (#1720 item 1)", async () => {
    const { store } = await chain(0);
    const body = "\u0001".repeat(10_000);
    await store.append(alice, { ref: REF, kind: "issue", key: "i1", data: { title: "t", body } });
    const { state } = await store.get(alice, { ref: REF });
    expect(state?.truncated).toEqual(["body"]);
    expect(Buffer.byteLength(JSON.stringify(state!.body))).toBeLessThanOrEqual(16 * 1024);
    expect(body.startsWith(state!.body!)).toBe(true);
    expect(state!.body!.length).toBeGreaterThan(2000);
  });

  it("a fresh install runs v1, v2 (principal_reissue) and v3 (the chain)", async () => {
    const pool = await database();
    const client = await pool.connect();
    try { expect(await migrate(client)).toBe(3); } finally { client.release(); }
    expect((await pool.query("SELECT version FROM records_schema ORDER BY version")).rows.map((row) => row.version)).toEqual([1, 2, 3]);
    expect((await pool.query("SELECT 1 FROM pg_proc WHERE proname = 'principal_reissue'")).rowCount).toBe(1);
    expect((await pool.query("SELECT 1 FROM information_schema.columns WHERE table_name = 'records' AND column_name = 'prev_hash'")).rowCount).toBe(1);
  });

  it.each([1, 2])("an existing v%i database with rows upgrades to v3 and its backfilled chain verifies", async (from) => {
    const pool = await database();
    const client = await pool.connect();
    try {
      // A database as #103 (v1) or #117 (v2) left it, with rows.
      await client.query("BEGIN");
      await client.query("CREATE TABLE records_schema (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())");
      for (let version = 1; version <= from; version++) {
        await client.query(MIGRATIONS[version - 1] as string);
        await client.query("INSERT INTO records_schema (version) VALUES ($1)", [version]);
      }
      for (let n = 1; n <= 1203; n++) {
        await client.query("INSERT INTO records (org, origin, seq, ref, kind, author, text, data, key, payload_hash) VALUES ('smarty-pants', 'dev1', $1, $2, 'comment', 'session:alice', $3, $4, $5, 'h')",
          [n, REF, `old ${n}`, JSON.stringify({ n, big: 12345678901234567890n.toString() }), `old${n}`]);
      }
      await client.query("COMMIT");
      expect(await migrate(client)).toBe(3);
    } finally { client.release(); }
    const store = new RecordStore(pool as unknown as ClientPool, { org: "smarty-pants", origin: "dev1" });
    await store.append(alice, { ref: REF, kind: "comment", key: "new", text: "after the migration" });
    expect(await store.verify(alice, { anchors: [await store.anchor(alice, {})] })).toMatchObject({ ok: true, clean: true, rows: 1204 });
    // The trigger is back on: records stay append-only for every role.
    await expect(pool.query("UPDATE records SET text = 'x' WHERE seq = 1")).rejects.toThrow(/append-only/);
  });
});

describe("fold-field sizing (#1720 item 1)", () => {
  it("cuts by the encoded size: an escape-heavy field fits the limit in JSON bytes", () => {
    const value = "\u0001".repeat(10_000);
    expect(Buffer.byteLength(value)).toBe(10_000);
    expect(Buffer.byteLength(JSON.stringify(value))).toBe(60_002);
    const cut = jsonPrefix(value, 16_384);
    expect(Buffer.byteLength(JSON.stringify(cut))).toBeLessThanOrEqual(16_384);
    expect(cut.length).toBe(Math.floor((16_384 - 2) / 6));
    expect(value.startsWith(cut)).toBe(true);
  });

  it("never splits a code point and keeps a field that fits", () => {
    const value = `${"😀".repeat(5000)}`;
    const cut = jsonPrefix(value, 16_384);
    expect(Buffer.byteLength(JSON.stringify(cut))).toBeLessThanOrEqual(16_384);
    expect([...cut].every((char) => char === "😀")).toBe(true);
    expect(jsonPrefix("short \"quoted\"", 16_384)).toBe("short \"quoted\"");
  });
});
