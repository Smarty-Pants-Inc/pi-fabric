import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { AdmissionGate, RecordArchiveLaggingError, type ArchiveFrontierProvider } from "../src/records/admission.js";
import { RecordsInbox, recordsInboxSession, recordsInboxMessage } from "../src/records/inbox.js";
import { PublicationRelay, type NudgePublisher } from "../src/records/relay.js";
import { migrate, WRITER_ROLE } from "../src/records/schema.js";
import { RecordKeyConflictError, RecordStore, type ClientPool, type RecordsPrincipal, type RecordStoreOptions } from "../src/records/store.js";
import { RecordsWatchdog } from "../src/records/watchdog.js";
import { bigIntToLsn, lsnToBigInt, WalGFrontierProvider } from "../src/records/admission.js";
import { SharedFrontierProvider } from "../src/records/shared-frontier.js";
import { postgresBin, startPostgres, type TestPostgres } from "./helpers/postgres.js";

const alice: RecordsPrincipal = { id: "session:alice", name: "alice" };
const bob: RecordsPrincipal = { id: "session:bob", name: "bob" };
const importer: RecordsPrincipal = { id: "importer", importer: true };
const REF = "Smarty-Pants-Inc/smarty-dev#754";

let server: TestPostgres;
let databases = 0;

const freshDatabase = async (max = 10): Promise<{ pool: pg.Pool; admin: pg.Pool }> => {
  const name = `records_${++databases}`;
  const root = server.pool({ max: 1 });
  await root.query(`CREATE DATABASE ${name}`);
  await root.end();
  const pool = new pg.Pool({ ...server.connection, database: name, max });
  pool.on("error", () => undefined);
  const client = await pool.connect();
  try { await migrate(client); } finally { client.release(); }
  return { pool, admin: pool };
};

const open: pg.Pool[] = [];
const storeFor = async (options: Partial<RecordStoreOptions> = {}, max = 10): Promise<{ store: RecordStore; pool: pg.Pool }> => {
  const { pool } = await freshDatabase(max);
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
    expect(got.state!.statuses["session:alice"]).toMatchObject({ id: aliceNew.id, text: "second", state: "done", name: "alice" });
    expect(got.state!.statuses["session:bob"]).toMatchObject({ state: "blocked", waitOn: "paul" });
    expect(got.state!.decisions.map((record) => record.id)).toEqual([revised.id]);
    expect(got.state!.openAsks.map((record) => record.id)).toEqual([ask2.id]);
    expect(got.state!.links).toHaveLength(1);
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

  it("refuses superseding another author's record and writes nothing; own edits and shared issue edits still work", async () => {
    const { store, pool } = await storeFor({ mirror: { enabled: true } });
    const counts = async () => (await pool.query("SELECT (SELECT count(*) FROM records)::int AS r, (SELECT count(*) FROM outbox)::int AS o, (SELECT count(*) FROM publication)::int AS p")).rows[0];
    const aliceStatus = await store.append(alice, { ref: REF, kind: "status", key: "s1", text: "alice's" });
    const imported = await store.append(importer, { ref: REF, kind: "comment", key: "gh-1", text: "from GitHub", author: "github:paul", data: { via: "github:paul" } });
    const before = await counts();
    // The reported sequence: bob's status supersedes alice's, which would PATCH alice's comment.
    await expect(store.append(bob, { ref: REF, kind: "status", key: "b1", text: "bob's", supersedes: aliceStatus.id })).rejects.toThrow(/by another author; only its author replaces it/);
    await expect(store.append(alice, { ref: REF, kind: "comment", key: "c", text: "rewrite", supersedes: imported.id })).rejects.toThrow(/by another author/);
    expect(await counts()).toEqual(before);
    // Counterexamples that must still work: the same author's edit, an imported author's edit, a shared issue.
    const edit = await store.append(alice, { ref: REF, kind: "status", key: "s2", text: "alice's, updated", supersedes: aliceStatus.id });
    expect((await pool.query("SELECT edit_of FROM outbox WHERE record_id = $1", [edit.id])).rows[0].edit_of).toBe(aliceStatus.id);
    await store.append(importer, { ref: REF, kind: "comment", key: "gh-1-edit", text: "edited on GitHub", author: "github:paul", supersedes: imported.id, data: { via: "github:paul" } });
    const issue = await store.append(alice, { ref: REF, kind: "issue", key: "i", data: { title: "Record", stage: "build" } });
    await store.append(bob, { ref: REF, kind: "issue", key: "stage", supersedes: issue.id, data: { stage: "review" } });
    expect((await store.get(bob, { ref: REF })).state!).toMatchObject({ title: "Record", stage: "review" });
  });

  it("commits nothing for an append aborted while it waits for the org lock or a connection", async () => {
    const { store, pool } = await storeFor({}, 2);
    const count = async () => (await pool.query("SELECT (SELECT count(*) FROM records)::int + (SELECT count(*) FROM outbox)::int + (SELECT count(*) FROM publication)::int AS n")).rows[0].n;
    const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
    // The reported sequence: blocked on the per-org lock, cancelled, then the lock frees.
    const blocker = await pool.connect();
    await blocker.query("BEGIN");
    await blocker.query("SELECT pg_advisory_xact_lock(hashtextextended('fabric-records:smarty-pants', 0))");
    const locked = new AbortController();
    const waiting = store.append(alice, { ref: REF, kind: "status", key: "late", text: "must not land" }, { signal: locked.signal });
    await sleep(300);
    locked.abort(new Error("call cancelled"));
    await expect(waiting).rejects.toThrow(/call cancelled/);
    await blocker.query("COMMIT");
    // Blocked on the pool (both connections busy), cancelled, then a connection frees.
    const busy = await pool.connect();
    const queued = new AbortController();
    const waitingForConnection = store.append(alice, { ref: REF, kind: "status", key: "late-2", text: "must not land" }, { signal: queued.signal });
    await sleep(100);
    queued.abort(new Error("call cancelled"));
    await expect(waitingForConnection).rejects.toThrow(/call cancelled/);
    busy.release();
    blocker.release();
    await sleep(500);
    expect(await count()).toBe(0);
    // Already cancelled: nothing starts.
    const gone = new AbortController();
    gone.abort(new Error("gone"));
    await expect(store.append(alice, { ref: REF, kind: "status", key: "late-3", text: "x" }, { signal: gone.signal })).rejects.toThrow(/gone/);
    // The counterexample: the pool still works, and an uncancelled append lands.
    expect((await store.append(alice, { ref: REF, kind: "status", key: "late", text: "must not land" })).sequence).toBe(1);
    expect(await count()).toBe(2);
  });

  it("commits nothing when the call is cancelled between the connection's arrival and the transaction", async () => {
    const { store: _unused, pool } = await storeFor();
    const cancelled = new AbortController();
    let queries = 0;
    // The store registers on the connection promise first; this cancel runs right after it,
    // before the transaction resumes: the handoff interval, where no abort listener exists.
    const handoffPool: ClientPool = {
      connect: () => {
        const pending = pool.connect() as unknown as Promise<import("../src/records/store.js").PooledClient>;
        void pending.then((client) => {
          const query = client.query.bind(client);
          client.query = ((...args: Parameters<typeof query>) => { queries++; return query(...args); }) as typeof client.query;
        });
        queueMicrotask(() => { void pending.then(() => cancelled.abort(new Error("cancelled in the handoff"))); });
        return pending;
      },
    };
    const store = new RecordStore(handoffPool, { org: "smarty-pants", origin: "dev1" });
    await expect(store.append(alice, { ref: REF, kind: "status", key: "handoff", text: "must not land" }, { signal: cancelled.signal })).rejects.toThrow(/cancelled in the handoff/);
    expect(queries).toBe(0); // not even BEGIN: the cancelled call starts no work
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect((await pool.query("SELECT count(*)::int AS n FROM records")).rows[0].n).toBe(0);
    // Counterexample: with no cancel, the same pool and store commit.
    const live = new RecordStore(pool as unknown as ClientPool, { org: "smarty-pants", origin: "dev1" });
    expect((await live.append(alice, { ref: REF, kind: "status", key: "handoff", text: "must not land" })).sequence).toBe(1);
  });

  it("PATCHes only the issue fields an update carries; creation sends the whole issue", async () => {
    const { store, pool } = await storeFor({ mirror: { enabled: true } });
    const outbox = async (id: string) => (await pool.query("SELECT method, endpoint, body, state FROM outbox WHERE record_id = $1", [id])).rows[0];
    const created = await store.append(alice, { repo: "Smarty-Pants-Inc/smarty-dev", kind: "issue", key: "new", data: { title: "Board", body: "The board" } });
    expect(await outbox(created.id)).toMatchObject({ method: "POST", endpoint: "/repos/Smarty-Pants-Inc/smarty-dev/issues", body: { title: "Board", body: `The board\n\n<!-- smarty-record:${created.id} -->` } });
    // The reported sequence: a title-only update without supersedes must not erase the forge body.
    const titleOnly = await store.append(alice, { ref: REF, kind: "issue", key: "title", data: { title: "Renamed" } });
    expect(await outbox(titleOnly.id)).toEqual({ method: "PATCH", endpoint: "/repos/Smarty-Pants-Inc/smarty-dev/issues/754", body: { title: "Renamed" }, state: "pending" });
    const labels = await store.append(alice, { ref: REF, kind: "issue", key: "labels", supersedes: titleOnly.id, data: { labels: ["p1"] } });
    expect((await outbox(labels.id)).body).toEqual({ labels: ["p1"] });
    const stage = await store.append(alice, { ref: REF, kind: "issue", key: "stage", data: { stage: "review" } });
    expect(await outbox(stage.id)).toMatchObject({ method: "PATCH", body: {}, state: "skipped" });
    // Counterexample: an update that carries a body sends it, with the marker.
    const body = await store.append(alice, { ref: REF, kind: "issue", key: "body", data: { body: "New body" } });
    expect((await outbox(body.id)).body).toEqual({ body: `New body\n\n<!-- smarty-record:${body.id} -->` });
  });

  it("stores record ids in one form, so mixed-case references still fold", async () => {
    const { store } = await storeFor();
    const mirrorer: RecordsPrincipal = { id: "mirror", mirror: true };
    const ask = await store.append(bob, { ref: REF, kind: "ask", key: "a", text: "host?", data: { to: "paul" } });
    await store.append(alice, { ref: REF, kind: "answer", key: "ans", text: "m4max", data: { ask: ask.id.toUpperCase(), outcome: "answered" } });
    expect((await store.get(alice, { ref: REF })).state!.openAsks).toEqual([]);
    await store.append(mirrorer, { ref: REF, kind: "mirror", key: "m1", data: { mirrorOf: ask.id.toUpperCase(), target: "github", state: "pending" } });
    await store.append(mirrorer, { ref: REF, kind: "mirror", key: "m2", data: { mirrorOf: ask.id, target: "github", state: "mirrored", githubId: "5858" } });
    expect((await store.get(alice, { ref: REF })).state!.mirror).toEqual({ [ask.id]: expect.objectContaining({ state: "mirrored", mirrorOf: ask.id }) });
    // The same payload spelled either way is one idempotent retry.
    const again = await store.append(alice, { ref: REF, kind: "answer", key: "ans", text: "m4max", data: { ask: ask.id, outcome: "answered" } });
    expect(again.sequence).toBe(2);
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

    it("runs each archive check once per interval across processes, and a failed check keeps the last good frontier", async () => {
      const { store, pool } = await storeFor();
      let calls = 0;
      let answer: string | Error = await insertLsn(pool);
      const inner: ArchiveFrontierProvider = { name: "m4max", frontier: async () => { calls++; if (answer instanceof Error) throw answer; return answer; } };
      const processA = new SharedFrontierProvider(inner, () => store, 60_000);
      const processB = new SharedFrontierProvider(inner, () => store, 60_000);
      const first = await processA.frontier();
      expect(await processB.frontier()).toBe(first);
      expect(calls).toBe(1);
      // The claim expires; the next claimer's check fails: the stored frontier stays the last good one.
      await pool.query("UPDATE archive_checks SET claimed_at = now() - interval '2 minutes'");
      answer = new Error("archive unreachable");
      await expect(processB.frontier()).rejects.toThrow(/unreachable/);
      expect(calls).toBe(2);
      expect(await processA.frontier()).toBe(first);
      expect((await pool.query("SELECT error FROM archive_checks")).rows[0].error).toBe("archive unreachable");
    });

    it("counts a record uncovered until its commit is archived, across a gate restart", async () => {
      let offset = 0;
      const target = fake("m4max");
      const now = () => Date.now() + offset;
      const { store, pool } = await storeFor();
      // Before the record, the WAL position; the record's own writes and COMMIT lie past it.
      const before = lsnToBigInt(await insertLsn(pool));
      await store.append(alice, { ref: REF, kind: "status", key: "s1", text: "acknowledged" });
      const bound = (await pool.query("SELECT bound::text AS bound FROM record_bounds")).rows[0].bound as string;
      expect(lsnToBigInt(bound)).toBeGreaterThan(before);
      // The reported sequence: only the WAL before the COMMIT is archived (the earlier segment).
      target.value = bigIntToLsn(before + 1n);
      offset = 302_000;
      // A new gate: a restarted process, with no in-memory samples.
      const restarted = new AdmissionGate({ providers: [target], now });
      const afterRestart = new RecordStore(pool as unknown as ClientPool, { org: "smarty-pants", origin: "dev1", admission: restarted });
      await expect(afterRestart.append(alice, { ref: REF, kind: "status", key: "s2", text: "next" })).rejects.toThrow(/record archive lagging 30\d s/);
      // Counterexample: once the archive holds the COMMIT, the old record is covered and appends land.
      target.value = bound;
      await restarted.refresh();
      expect((await afterRestart.append(alice, { ref: REF, kind: "status", key: "s2", text: "next" })).sequence).toBe(2);
    });

    it("bounds a record a crash left without one, at the next service start", async () => {
      const { store, pool } = await storeFor();
      await store.append(alice, { ref: REF, kind: "status", key: "s1", text: "one" });
      await store.append(alice, { ref: REF, kind: "status", key: "s2", text: "two" });
      // The crash window: s1 committed but was never bounded, and a newer record was.
      await pool.query("DELETE FROM record_bounds WHERE seq = 1");
      await store.transaction((client) => store.fillBounds(client, "recent"));
      expect((await pool.query("SELECT count(*)::int AS n FROM record_bounds")).rows[0].n).toBe(1);
      await store.transaction((client) => store.fillBounds(client, "all"));
      expect((await pool.query("SELECT count(*)::int AS n FROM record_bounds")).rows[0].n).toBe(2);
    });

    it("admits by the WAL-G report's contiguous FOUND prefix: a missing middle segment cuts it, newer segments do not count", async () => {
      let offset = 0;
      const { store, pool } = await storeFor();
      const walfile = async (sql: string, values: unknown[] = []) => (await pool.query(`SELECT pg_walfile_name(${sql}) AS name`, values)).rows[0].name as string;
      // Record A's COMMIT is in segment N; record B's in N+1; the insert position moves on to N+2.
      await store.append(alice, { ref: REF, kind: "status", key: "a", text: "in segment N" });
      const boundA = (await pool.query("SELECT bound::text AS bound FROM record_bounds b JOIN records r ON r.id = b.record_id WHERE r.key = 'a'")).rows[0].bound as string;
      await pool.query("SELECT pg_switch_wal()");
      await store.append(alice, { ref: REF, kind: "status", key: "b", text: "in segment N+1" });
      const boundB = (await pool.query("SELECT bound::text AS bound FROM record_bounds b JOIN records r ON r.id = b.record_id WHERE r.key = 'b'")).rows[0].bound as string;
      await pool.query("SELECT pg_switch_wal()");
      const segA = await walfile("$1::pg_lsn", [boundA]);
      const segB = await walfile("$1::pg_lsn", [boundB]);
      expect(segB > segA).toBe(true);
      const before = await walfile("$1::pg_lsn - 16777216", [boundA]);
      const first = `${segA.slice(0, 8)}${"0".repeat(15)}1`;
      const range = (start: string, end: string, status: string) => ({ timeline_id: 1, start_segment: start, end_segment: end, segments_count: 1, status });
      // wal-g wal-verify integrity --json: the current segment is not listed; N is lost, N+1 is present.
      let report: unknown = { integrity: { status: "WARNING", details: [range(first, before, "FOUND"), range(segA, segA, "MISSING_LOST"), range(segB, segB, "FOUND")] } };
      const walg = new WalGFrontierProvider("m4max", ["wal-g", "wal-verify", "integrity", "--json"], {
        run: async () => ({ stdout: JSON.stringify(report), stderr: "", code: 0 }),
      });
      const gate = new AdmissionGate({ providers: [walg], now: () => Date.now() + offset });
      const gated = new RecordStore(pool as unknown as ClientPool, { org: "smarty-pants", origin: "dev1", admission: gate });
      await gate.refresh();
      // The frontier stops before the gap: the start of segment N, not the end of N+1.
      expect(gate.frontier()).toBe(bigIntToLsn(lsnToBigInt(boundA) & ~(16n * 1024n * 1024n - 1n)));
      offset = 302_000;
      await expect(gated.append(alice, { ref: REF, kind: "status", key: "c", text: "next" })).rejects.toThrow(/^record archive lagging 30\d s; retry with the same key$/);
      // Counterexample: segment N is archived after all; the prefix now runs through N+1, covering A and B.
      report = { integrity: { status: "OK", details: [range(first, segB, "FOUND")] } };
      await gate.refresh();
      expect(lsnToBigInt(gate.frontier()!)).toBeGreaterThan(lsnToBigInt(boundB));
      expect((await gated.append(alice, { ref: REF, kind: "status", key: "c", text: "next" })).sequence).toBe(3);
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

    it("replays a pending batch by its ids after the root is renamed", async () => {
      const { store } = await storeFor();
      let names = ["bob"];
      const inbox = new RecordsInbox(store, bob.id, () => names);
      await inbox.next(recordsInboxSession([]));
      const old = await store.append(alice, { ref: REF, kind: "ask", key: "old", text: "to the old name", data: { to: "bob" } });
      expect((await inbox.next(recordsInboxSession([]))).records.map((record) => record.id)).toEqual([old.id]);
      // Interrupted before the session held it; then the root is renamed; then a newer record comes.
      names = ["robert"];
      const newer = await store.append(alice, { ref: REF, kind: "handoff", key: "new", data: { to: bob.id } });
      const replay = await inbox.next(recordsInboxSession([]));
      expect(replay.records.map((record) => record.id)).toEqual([old.id]);
      const held = [{ type: "custom_message", ...recordsInboxMessage(replay.records) }];
      expect((await inbox.next(recordsInboxSession(held))).records.map((record) => record.id)).toEqual([newer.id]);
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
      // Re-alarms are bounded, across processes too: another process's watchdog does not repeat it.
      await watchdog.tick();
      const otherAlarms: string[] = [];
      await new RecordsWatchdog({ store, now: () => Date.now() + offset, alarm: async (lag) => { otherAlarms.push(lag.consumer); } }).tick();
      expect(alarms).toEqual(["session:carol"]);
      expect(otherAlarms).toEqual([bob.id]); // bob lags for it too; carol is not repeated
      offset = 121_000 + 11 * 60_000;
      await watchdog.tick();
      expect(alarms).toEqual(["session:carol", "session:carol"]);
      const batch = await mine.next(recordsInboxSession([]));
      await mine.next(recordsInboxSession([{ type: "custom_message", ...recordsInboxMessage(batch.records) }]));
      expect((await watchdog.lagging()).map((lag) => lag.consumer)).toEqual(["session:carol"]);
    });
  });
});
