import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { AdmissionGate, RecordArchiveLaggingError, type ArchiveFrontierProvider } from "../src/records/admission.js";
import { RecordsInbox, recordsInboxSession, recordsInboxMessage } from "../src/records/inbox.js";
import { PublicationRelay, type NudgePublisher } from "../src/records/relay.js";
import { migrate, WRITER_ROLE } from "../src/records/schema.js";
import { RecordKeyConflictError, RecordStore, type ClientPool, type RecordsPrincipal, type RecordStoreOptions } from "../src/records/store.js";
import { RecordsWatchdog } from "../src/records/watchdog.js";
import { postgresBin, startPostgres, type TestPostgres } from "./helpers/postgres.js";

const alice: RecordsPrincipal = { id: "session:alice", name: "alice" };
const bob: RecordsPrincipal = { id: "session:bob", name: "bob" };
const importer: RecordsPrincipal = { id: "importer", importer: true };
const REF = "Smarty-Pants-Inc/smarty-dev#754";

let server: TestPostgres;
let databases = 0;

const freshDatabase = async (): Promise<{ pool: pg.Pool; admin: pg.Pool }> => {
  const name = `records_${++databases}`;
  const root = server.pool({ max: 1 });
  await root.query(`CREATE DATABASE ${name}`);
  await root.end();
  const pool = new pg.Pool({ ...server.connection, database: name, max: 10 });
  pool.on("error", () => undefined);
  const client = await pool.connect();
  try { await migrate(client); } finally { client.release(); }
  return { pool, admin: pool };
};

const open: pg.Pool[] = [];
const storeFor = async (options: Partial<RecordStoreOptions> = {}): Promise<{ store: RecordStore; pool: pg.Pool }> => {
  const { pool } = await freshDatabase();
  open.push(pool);
  return { store: new RecordStore(pool as unknown as ClientPool, { org: "smarty-pants", origin: "dev1", ...options }), pool };
};

const recorder = (): NudgePublisher & { events: { topic: string; kind: string; to?: string; data: Record<string, unknown> }[] } => {
  const events: { topic: string; kind: string; to?: string; data: Record<string, unknown> }[] = [];
  return { events, publish: async (input) => { events.push(input); return { sequence: events.length }; } };
};

describe.skipIf(!postgresBin)("records on a real PostgreSQL", () => {
  beforeAll(async () => { server = await startPostgres(); }, 60_000);
  afterAll(async () => {
    await Promise.allSettled(open.map((pool) => pool.end()));
    await server?.stop();
  }, 30_000);

  it("appends, returns the receipt after commit, and reads by cursor up to the frontier", async () => {
    const { store } = await storeFor();
    const first = await store.append(alice, { ref: REF, kind: "status", key: "s1", text: "on it", data: { state: "in progress", eta: "~08:00Z PR" } });
    expect(first).toMatchObject({ sequence: 1, origin: "dev1", ref: REF, topic: "record/Smarty-Pants-Inc/smarty-dev/754", key: "s1" });
    await store.append(bob, { ref: REF, kind: "ask", key: "a1", text: "which host?", data: { to: "paul", class: "decision" } });
    await store.append(bob, { ref: REF, kind: "comment", key: "c1", text: "merged" });
    const page1 = await store.read(alice, { after: 0, limit: 2 });
    expect(page1.records.map((record) => record.sequence)).toEqual([1, 2]);
    expect(page1).toMatchObject({ next: 2, frontier: 3 });
    expect(page1.records[0]).toMatchObject({ from: "session:alice", fromName: "alice", kind: "status", data: { state: "in progress" } });
    const page2 = await store.read(alice, { after: page1.next, limit: 2 });
    expect(page2.records.map((record) => record.sequence)).toEqual([3]);
    expect(page2.next).toBe(3);
    const empty = await store.read(alice, { after: 3 });
    expect(empty).toMatchObject({ records: [], next: 3, frontier: 3 });
    expect((await store.read(alice, { after: 0, to: "paul" })).records.map((record) => record.key)).toEqual(["a1"]);
  });

  it("commits in seq order under concurrent appends, so a cursor reader never skips a record", async () => {
    const { store, pool } = await storeFor();
    const total = 60;
    const seen: number[] = [];
    let cursor = 0;
    let done = false;
    const reader = (async () => {
      while (!done || cursor < total) {
        const page = await store.read(alice, { after: cursor, limit: 500 });
        for (const record of page.records) seen.push(record.sequence);
        cursor = page.next;
      }
    })();
    const receipts = await Promise.all(Array.from({ length: total }, (_, index) =>
      store.append(index % 2 ? alice : bob, { ref: REF, kind: "comment", key: `k${index}`, text: `c${index}` })));
    done = true;
    await reader;
    expect(receipts.map((receipt) => receipt.sequence).sort((a, b) => a - b)).toEqual(Array.from({ length: total }, (_, index) => index + 1));
    expect(seen).toEqual(Array.from({ length: total }, (_, index) => index + 1));
    // Commit order equals seq order: commit timestamps never decrease along seq.
    const { rows } = await pool.query<{ seq: string; committed: Date }>("SELECT seq, pg_xact_commit_timestamp(xmin) AS committed FROM records ORDER BY seq");
    for (let index = 1; index < rows.length; index++) expect(rows[index]!.committed.getTime()).toBeGreaterThanOrEqual(rows[index - 1]!.committed.getTime());
  });

  it("returns the original receipt for an identical retry and refuses a different payload under the key (C3)", async () => {
    const { store, pool } = await storeFor();
    const args = { ref: REF, kind: "status", key: "status-1", text: "waiting", data: { state: "waiting", waitOn: "org" } };
    const receipt = await store.append(alice, args);
    // Field order does not change the payload.
    expect(await store.append(alice, { data: { waitOn: "org", state: "waiting" }, text: "waiting", key: "status-1", kind: "status", ref: REF })).toEqual(receipt);
    await expect(store.append(alice, { ...args, text: "blocked" })).rejects.toBeInstanceOf(RecordKeyConflictError);
    await expect(store.append(alice, { ...args, data: { state: "blocked" } })).rejects.toThrow(/already used for a different payload/);
    // The key is per author: another author may use the same key.
    const other = await store.append(bob, args);
    expect(other.id).not.toBe(receipt.id);
    expect((await pool.query("SELECT count(*)::int AS n FROM records")).rows[0].n).toBe(2);
    expect((await pool.query("SELECT count(*)::int AS n FROM publication")).rows[0].n).toBe(2);
  });

  it("takes the author from the caller; only the importer sets it, with data.via (C13)", async () => {
    const { store } = await storeFor();
    await expect(store.append(alice, { ref: REF, kind: "comment", key: "x", text: "hi", author: "session:bob" })).rejects.toThrow(/only the importer role/);
    await expect(store.append(alice, { ref: REF, kind: "comment", key: "x", text: "hi", data: { via: "github:bot" } })).rejects.toThrow(/importer role/);
    await expect(store.append(importer, { ref: REF, kind: "comment", key: "x", text: "hi", author: "github:paul" })).rejects.toThrow(/needs data.via/);
    await expect(store.append(alice, { ref: REF, kind: "mirror", key: "m", data: { mirrorOf: "00000000-0000-4000-8000-000000000000", target: "github", state: "mirrored" } })).rejects.toThrow(/mirror role/);
    const imported = await store.append(importer, { ref: REF, kind: "comment", key: "gh-1", text: "from GitHub", author: "github:paul", data: { via: "github:smarty-fleet-write[bot]", githubId: "5858" } });
    const own = await store.append(importer, { ref: REF, kind: "comment", key: "own", text: "by the importer itself" });
    const { records } = await store.read(alice, { after: 0 });
    expect(records.find((record) => record.id === imported.id)).toMatchObject({ from: "github:paul", data: { via: "github:smarty-fleet-write[bot]" } });
    expect(records.find((record) => record.id === own.id)?.from).toBe("importer");
  });

  it("refuses unknown kinds and fields, and checks references", async () => {
    const { store } = await storeFor();
    await expect(store.append(alice, { ref: REF, kind: "note", key: "k" })).rejects.toThrow(/kind must be one of/);
    await expect(store.append(alice, { ref: REF, kind: "status", key: "k", data: { mood: "good" } })).rejects.toThrow(/unknown field data.mood/);
    await expect(store.append(alice, { ref: REF, kind: "status", key: "k", extra: 1 })).rejects.toThrow(/unknown field "extra"/);
    await expect(store.append(alice, { ref: "smarty-dev#754", kind: "status", key: "k" })).rejects.toThrow(/ref must look like/);
    await expect(store.append(alice, { ref: REF, kind: "status" })).rejects.toThrow(/key is required/);
    await expect(store.append(alice, { ref: REF, kind: "answer", key: "k", data: { ask: "00000000-0000-4000-8000-000000000000", outcome: "answered" } })).rejects.toThrow(/must name an ask/);
    const other = await store.append(alice, { ref: "Smarty-Pants-Inc/smarty-dev#1", kind: "status", key: "s" });
    await expect(store.append(alice, { ref: REF, kind: "status", key: "k", supersedes: other.id })).rejects.toThrow(/supersedes must name a status record on/);
  });

  it("allocates Node-native refs for new issues (C11)", async () => {
    const { store } = await storeFor();
    const first = await store.append(alice, { repo: "Smarty-Pants-Inc/smarty-dev", kind: "issue", key: "i1", data: { title: "Record layer" } });
    const second = await store.append(alice, { repo: "Smarty-Pants-Inc/smarty-dev", kind: "issue", key: "i2", data: { title: "Board" } });
    expect([first.ref, second.ref]).toEqual(["Smarty-Pants-Inc/smarty-dev#L1", "Smarty-Pants-Inc/smarty-dev#L2"]);
    expect(first.topic).toBe("record/Smarty-Pants-Inc/smarty-dev/L1");
    expect(await store.append(alice, { repo: "Smarty-Pants-Inc/smarty-dev", kind: "issue", key: "i1", data: { title: "Record layer" } })).toEqual(first);
  });

  it("folds: newest status per author, merged issue fields, supersedes, open asks, close", async () => {
    const { store } = await storeFor();
    const issue = await store.append(alice, { ref: REF, kind: "issue", key: "i", data: { title: "Record", owner: "fabric-v2", stage: "build", labels: ["p1"] } });
    await store.append(alice, { ref: REF, kind: "issue", key: "i-stage", supersedes: issue.id, data: { stage: "review" } });
    await store.append(alice, { ref: REF, kind: "status", key: "s1", text: "first", data: { state: "in progress" } });
    await store.append(bob, { ref: REF, kind: "status", key: "s1", text: "bob", data: { state: "blocked", waitOn: "paul" } });
    const aliceNew = await store.append(alice, { ref: REF, kind: "status", key: "s2", text: "second", data: { state: "done", eta: [{ stage: "installed", at: "2026-09-28T08:00Z" }] } });
    const decision = await store.append(alice, { ref: REF, kind: "decision", key: "d1", text: "Postgres", data: { by: "paul" } });
    const revised = await store.append(alice, { ref: REF, kind: "decision", key: "d2", text: "PostgreSQL 17", supersedes: decision.id });
    const ask1 = await store.append(bob, { ref: REF, kind: "ask", key: "a1", text: "host?", data: { to: "paul" } });
    const ask2 = await store.append(bob, { ref: REF, kind: "ask", key: "a2", text: "budget?", data: { to: "paul" } });
    await store.append(importer, { ref: REF, kind: "answer", key: "ans", text: "m4max", author: "github:paul", data: { ask: ask1.id, outcome: "answered", via: "github:paul" } });
    await store.append(alice, { ref: REF, kind: "link", key: "l", data: { pr: "Smarty-Pants-Inc/pi-fabric#102" } });
    const got = await store.get(alice, { ref: REF });
    expect(got.state).toMatchObject({ title: "Record", owner: "fabric-v2", stage: "review", labels: ["p1"], open: true });
    expect(got.state.statuses["session:alice"]).toMatchObject({ id: aliceNew.id, text: "second", state: "done", name: "alice" });
    expect(got.state.statuses["session:bob"]).toMatchObject({ state: "blocked", waitOn: "paul" });
    expect(got.state.decisions.map((record) => record.id)).toEqual([revised.id]);
    expect(got.state.openAsks.map((record) => record.id)).toEqual([ask2.id]);
    expect(got.state.links).toHaveLength(1);
    expect(got.history).toHaveLength(11);
    const paged = await store.get(alice, { ref: REF, limit: 4 });
    expect(paged.history).toHaveLength(4);
    const rest = await store.get(alice, { ref: REF, after: paged.next!, limit: 50 });
    expect(rest.history[0]!.sequence).toBe(paged.history[3]!.sequence + 1);
    expect(rest.next).toBeUndefined();

    await store.append(alice, { ref: "Smarty-Pants-Inc/smarty-dev#1", kind: "issue", key: "i-1", data: { title: "Other", owner: "dev-lead" } });
    await store.append(alice, { ref: "Smarty-Pants-Inc/smarty-dev#1", kind: "close", key: "close-1", data: { reason: "done" } });
    const list = await store.list(alice, { repo: "Smarty-Pants-Inc/smarty-dev" });
    expect(list.items.map((item) => item.ref)).toEqual(["Smarty-Pants-Inc/smarty-dev#1", REF]);
    expect(list.items[1]).toMatchObject({ title: "Record", open: true, statuses: { "session:alice": { state: "done" }, "session:bob": { state: "blocked" } }, openAsks: [{ id: ask2.id, to: "paul" }] });
    expect((await store.list(alice, { open: true })).items.map((item) => item.ref)).toEqual([REF]);
    expect((await store.list(alice, { owner: "dev-lead" })).items.map((item) => item.ref)).toEqual(["Smarty-Pants-Inc/smarty-dev#1"]);
    expect((await store.list(alice, { hasOpenAsk: true })).items.map((item) => item.ref)).toEqual([REF]);
    const first = await store.list(alice, { limit: 1 });
    expect(first.items).toHaveLength(1);
    const second = await store.list(alice, { limit: 1, after: first.next! });
    expect(second.items.map((item) => item.ref)).toEqual([REF]);
    expect(second.next).toBeUndefined();
  });

  it("writes the outbox row in the record's transaction, with edit_of and the replay guard (Appendix B)", async () => {
    const { store, pool } = await storeFor({ mirror: { enabled: true, repos: ["Smarty-Pants-Inc/smarty-dev"] } });
    const s1 = await store.append(alice, { ref: REF, kind: "status", key: "s1", text: "one" });
    const s2 = await store.append(alice, { ref: REF, kind: "status", key: "s2", text: "two" });
    const c1 = await store.append(alice, { ref: REF, kind: "comment", key: "c1", text: "result" });
    const c2 = await store.append(alice, { ref: REF, kind: "comment", key: "c2", text: "result, fixed", supersedes: c1.id });
    await store.append(alice, { ref: REF, kind: "handoff", key: "h", data: { to: "dev-lead" } });
    await store.append(importer, { ref: REF, kind: "comment", key: "gh", text: "imported", author: "github:paul", data: { via: "github:paul" } });
    await store.append(alice, { ref: "Smarty-Pants-Inc/pi-fabric#5", kind: "comment", key: "elsewhere", text: "not mirrored here" });
    await store.append(alice, { ref: REF, kind: "close", key: "close", data: { reason: "done" } });
    const { rows } = await pool.query("SELECT record_id, method, endpoint, state, edit_of, request_key, target_thread, who, owner, repo, body FROM outbox ORDER BY seq");
    expect(rows.map((row) => [row.method, row.endpoint, row.state])).toEqual([
      ["POST", "/repos/Smarty-Pants-Inc/smarty-dev/issues/754/comments", "pending"],
      ["PATCH", "/repos/Smarty-Pants-Inc/smarty-dev/issues/comments/{github_id}", "pending"],
      ["POST", "/repos/Smarty-Pants-Inc/smarty-dev/issues/754/comments", "pending"],
      ["PATCH", "/repos/Smarty-Pants-Inc/smarty-dev/issues/comments/{github_id}", "pending"],
      ["POST", "/repos/Smarty-Pants-Inc/pi-fabric/issues/5/comments", "skipped"],
      ["PATCH", "/repos/Smarty-Pants-Inc/smarty-dev/issues/754", "pending"],
    ]);
    expect(rows[1]).toMatchObject({ record_id: s2.id, edit_of: s1.id, target_thread: REF, who: "session:alice", owner: "Smarty-Pants-Inc", repo: "smarty-dev" });
    expect(rows[3]).toMatchObject({ record_id: c2.id, edit_of: c1.id });
    expect(rows[0].body.body).toBe(`one\n\n<!-- smarty-record:${s1.id} -->`);
    expect(rows[0].request_key).toBe(`record:${s1.id}`);
    // Replaying a record is a no-op at the database: both unique keys hold.
    await expect(pool.query("INSERT INTO outbox (owner, repo, method, endpoint, target_thread, who, record_id, request_key) VALUES ($1, 'smarty-dev', 'POST', '/x', $2, 'w', $3, 'other')", ["Smarty-Pants-Inc", REF, s1.id])).rejects.toThrow(/duplicate key/);
    await expect(pool.query("INSERT INTO outbox (owner, repo, method, endpoint, target_thread, who, request_key) VALUES ('Smarty-Pants-Inc', 'smarty-dev', 'POST', '/x', 't', 'w', $1)", [`record:${s1.id}`])).rejects.toThrow(/duplicate key/);
    // A failed append writes neither the record nor its outbox row.
    await expect(store.append(alice, { ref: REF, kind: "status", key: "s1", text: "changed" })).rejects.toBeInstanceOf(RecordKeyConflictError);
    expect((await pool.query("SELECT count(*)::int AS n FROM outbox")).rows[0].n).toBe(6);
  });

  it("writes no outbox rows with the mirror off", async () => {
    const { store, pool } = await storeFor();
    await store.append(alice, { ref: REF, kind: "status", key: "s", text: "x" });
    expect((await pool.query("SELECT count(*)::int AS n FROM outbox")).rows[0].n).toBe(0);
  });

  it("publishes nudges after commit and republishes after a crash (commit, then nudge)", async () => {
    const { store, pool } = await storeFor();
    const failing: NudgePublisher = { publish: async () => { throw new Error("mesh down"); } };
    await store.append(alice, { ref: REF, kind: "ask", key: "a", text: "ask", data: { to: "paul" } });
    await store.append(alice, { ref: REF, kind: "status", key: "s", text: "status" });
    expect(await new PublicationRelay(store, failing).flush()).toEqual({ published: 0, failed: 1 });
    expect(await new PublicationRelay(store, failing).unpublished()).toBe(2);
    // A crash after the mesh took the event but before the row was marked: published again.
    const crashAfterPublish = recorder();
    const crashing: NudgePublisher = { publish: async (input) => { await crashAfterPublish.publish(input); throw new Error("killed"); } };
    await new PublicationRelay(store, crashing).flush();
    expect(crashAfterPublish.events).toHaveLength(1);
    // The restarted process's relay publishes every unpublished row, in seq order.
    const mesh = recorder();
    expect(await new PublicationRelay(store, mesh).flush()).toEqual({ published: 2, failed: 0 });
    expect(mesh.events.map((event) => [event.topic, event.kind, event.to])).toEqual([
      ["record/Smarty-Pants-Inc/smarty-dev/754", "record.ask", "paul"],
      ["record/Smarty-Pants-Inc/smarty-dev/754", "record.status", undefined],
    ]);
    expect(mesh.events[0]!.data).toMatchObject({ ref: REF, sequence: 1, from: "session:alice", key: "a" });
    expect(await new PublicationRelay(store, mesh).flush()).toEqual({ published: 0, failed: 0 });
    const { rows } = await pool.query("SELECT mesh_sequence, published_at IS NOT NULL AS published FROM publication ORDER BY seq");
    expect(rows).toEqual([{ mesh_sequence: "1", published: true }, { mesh_sequence: "2", published: true }]);
  });

  it("refuses UPDATE, DELETE and TRUNCATE of records by grant and by trigger", async () => {
    const { store, pool } = await storeFor();
    await store.append(alice, { ref: REF, kind: "comment", key: "c", text: "keep" });
    for (const statement of ["UPDATE records SET text = 'changed'", "DELETE FROM records", "TRUNCATE records CASCADE"]) {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        await client.query(`SET LOCAL ROLE ${WRITER_ROLE}`);
        await expect(client.query(statement)).rejects.toThrow(/permission denied/);
      } finally {
        await client.query("ROLLBACK");
        client.release();
      }
      // Even the owner (a superuser here) is refused by the append-only trigger.
      await expect(pool.query(statement)).rejects.toThrow(/append-only/);
    }
    const { rows } = await pool.query("SELECT text FROM records");
    expect(rows).toEqual([{ text: "keep" }]);
    // The writer may insert and select, and nothing else on records.
    const grants = await pool.query("SELECT privilege_type FROM information_schema.role_table_grants WHERE grantee = $1 AND table_name = 'records' ORDER BY 1", [WRITER_ROLE]);
    expect(grants.rows.map((row) => row.privilege_type)).toEqual(["INSERT", "SELECT"]);
  });

  describe("C2 admission", () => {
    const fake = (name: string): ArchiveFrontierProvider & { value: string | undefined } => {
      const provider = { name, value: undefined as string | undefined, frontier: async () => provider.value };
      return provider;
    };
    const insertLsn = async (pool: pg.Pool): Promise<string> => (await pool.query("SELECT pg_current_wal_insert_lsn()::text AS lsn")).rows[0].lsn;

    it("alarms at 2 min, refuses past 5 min with a retryable error, and the fresher target counts", async () => {
      let offset = 0;
      const m4 = fake("m4max");
      const b2 = fake("b2");
      const statuses: string[] = [];
      const gate = new AdmissionGate({ providers: [m4, b2], now: () => Date.now() + offset, onStatus: (status) => statuses.push(status.state) });
      const { store, pool } = await storeFor({ admission: gate });
      m4.value = b2.value = await insertLsn(pool);
      await gate.refresh();
      const first = await store.append(alice, { ref: REF, kind: "status", key: "s1", text: "one" });
      expect(gate.status()).toMatchObject({ state: "ok", lagSeconds: 0 });
      // Archiving stops: the frontier stays behind the new records.
      offset = 121_000;
      await store.append(alice, { ref: REF, kind: "status", key: "s2", text: "two" });
      expect(gate.status()).toMatchObject({ state: "alarm" });
      expect(gate.status()!.lagSeconds).toBeGreaterThanOrEqual(120);
      offset = 302_000;
      const refused = store.append(alice, { ref: REF, kind: "status", key: "s3", text: "three" });
      await expect(refused).rejects.toBeInstanceOf(RecordArchiveLaggingError);
      await expect(store.append(alice, { ref: REF, kind: "status", key: "s3", text: "three" })).rejects.toThrow(/^record archive lagging 30\d s; retry with the same key$/);
      expect(statuses).toContain("refuse");
      // An identical retry of a committed record still gets its receipt while refusing.
      expect(await store.append(alice, { ref: REF, kind: "status", key: "s1", text: "one" })).toEqual(first);
      // One target catches up (the other stays stale): the fresher one counts, and the retry lands.
      b2.value = await insertLsn(pool);
      await gate.refresh();
      const landed = await store.append(alice, { ref: REF, kind: "status", key: "s3", text: "three" });
      expect(landed.sequence).toBe(3);
      expect(gate.status()).toMatchObject({ state: "ok", frontier: b2.value });
      expect((await pool.query("SELECT count(*)::int AS n FROM records")).rows[0].n).toBe(3);
    });

    it("refuses when no target ever answered and records are old (fail closed)", async () => {
      let offset = 0;
      const gate = new AdmissionGate({ providers: [fake("m4max")], now: () => Date.now() + offset });
      const { store } = await storeFor({ admission: gate });
      await store.append(alice, { ref: REF, kind: "comment", key: "c1", text: "one" });
      offset = 400_000;
      await expect(store.append(alice, { ref: REF, kind: "comment", key: "c2", text: "two" })).rejects.toThrow(/record archive lagging/);
    });
  });

  describe("reconcile by processing cursor, and the idle watchdog (C4)", () => {
    it("delivers addressed records at least once and advances the cursor only once the session holds them", async () => {
      const { store, pool } = await storeFor();
      await store.append(alice, { ref: REF, kind: "ask", key: "before", text: "old", data: { to: "bob" } });
      const inbox = new RecordsInbox(store, bob.id, () => ["bob"]);
      // A new consumer starts at the present.
      expect((await inbox.next(recordsInboxSession([]))).records).toEqual([]);
      const ask = await store.append(alice, { ref: REF, kind: "ask", key: "a", text: "which host?", data: { to: "bob" } });
      await store.append(alice, { ref: REF, kind: "comment", key: "c", text: "not for bob" });
      await store.append(bob, { ref: REF, kind: "handoff", key: "own", data: { to: "bob" } });
      const handoff = await store.append(alice, { ref: REF, kind: "handoff", key: "h", data: { to: bob.id } });
      const batch = await inbox.next(recordsInboxSession([]));
      expect(batch.records.map((record) => record.id)).toEqual([ask.id, handoff.id]);
      // Not yet in the session (a stop before it was written): delivered again.
      expect((await inbox.next(recordsInboxSession([]))).records.map((record) => record.id)).toEqual([ask.id, handoff.id]);
      const cursorBefore = (await pool.query("SELECT after FROM consumers WHERE consumer = $1", [bob.id])).rows[0].after;
      expect(Number(cursorBefore)).toBe(1);
      const entries = [{ type: "custom_message", ...recordsInboxMessage(batch.records) }];
      expect((await inbox.next(recordsInboxSession(entries))).records).toEqual([]);
      expect(Number((await pool.query("SELECT after FROM consumers WHERE consumer = $1", [bob.id])).rows[0].after)).toBe(5);
    });

    it("wakes its own lagging root, alarms for another, and republishes unpublished nudges", async () => {
      const { store } = await storeFor();
      let offset = 0;
      const mine = new RecordsInbox(store, bob.id, () => ["bob"]);
      const theirs = new RecordsInbox(store, "session:carol", () => ["carol"]);
      await mine.next(recordsInboxSession([]));
      await theirs.next(recordsInboxSession([]));
      await store.append(alice, { ref: REF, kind: "ask", key: "b", text: "for bob", data: { to: "bob" } });
      await store.append(alice, { ref: REF, kind: "ask", key: "c", text: "for carol", data: { to: "carol" } });
      const mesh = recorder();
      const woke: string[] = [];
      const alarms: string[] = [];
      const watchdog = new RecordsWatchdog({
        store, relay: new PublicationRelay(store, mesh), self: bob.id, now: () => Date.now() + offset,
        wake: async () => { woke.push(bob.id); }, alarm: async (lag) => { alarms.push(lag.consumer); },
      });
      const early = await watchdog.tick();
      expect(early.lagging).toEqual([]);
      expect(mesh.events).toHaveLength(2);
      offset = 121_000;
      const late = await watchdog.tick();
      expect(late.lagging.map((lag) => lag.consumer).sort()).toEqual([bob.id, "session:carol"]);
      expect(woke).toEqual([bob.id]);
      expect(alarms).toEqual(["session:carol"]);
      // Re-alarms are bounded; a consumer that caught up is no longer lagging.
      await watchdog.tick();
      expect(alarms).toEqual(["session:carol"]);
      const batch = await mine.next(recordsInboxSession([]));
      await mine.next(recordsInboxSession([{ type: "custom_message", ...recordsInboxMessage(batch.records) }]));
      expect((await watchdog.lagging()).map((lag) => lag.consumer)).toEqual(["session:carol"]);
    });
  });
});
