import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { openNodeSqlite, SqliteStateStore, type SqliteConnection, type SqliteRow, type SqliteStateStoreOptions } from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshBatchOperation, type MeshIdentity } from "../src/mesh/store.js";

// Keep reason: the real retained-feed scan behind prefix proofs (smarty-dev#16879), plus data-safety
// equivalence at whole-commit boundaries. Inspect SQLite's executable plan, never a wall-time budget.
const identity: MeshIdentity = { id: "feed-reader", name: "feed-reader", kind: "agent" };
const LEGACY_SINCE = "SELECT commit_no, key, version, deleted FROM changes WHERE commit_no > ? ORDER BY seq";
const LEGACY_OLDEST = "SELECT min(commit_no) AS oldest FROM changes";
const roots: string[] = [];
const handles: Array<{ close(): void }> = [];
const root = (): string => {
  const value = fs.mkdtempSync(path.join(os.tmpdir(), "pi-feed-read-"));
  roots.push(value);
  return value;
};
const open = async (where: string, options: SqliteStateStoreOptions = {}) => {
  const store = await SqliteStateStore.open(where, 64 * 1024, 1_000, { initialize: "create", checkpointLog: "off", walResetBytes: 0, ...options });
  handles.push(store);
  return store;
};
const raw = (where: string): SqliteConnection => {
  const db = openNodeSqlite(path.join(where, "state.db"));
  handles.push(db);
  return db;
};
const captured = async () => {
  const where = root();
  const statements: string[] = [];
  const store = await open(where, { open: file => {
    const db = openNodeSqlite(file);
    const prepare = db.prepare.bind(db);
    db.prepare = sql => { statements.push(sql); return prepare(sql); };
    return db;
  } });
  const since = statements.find(sql => sql.startsWith("SELECT commit_no, key, version, deleted FROM changes"));
  const oldest = statements.find(sql => sql.includes(" AS oldest FROM changes"));
  if (!since || !oldest) throw new Error("feed statements were not prepared by the real store");
  return { store, db: raw(where), since, oldest };
};
const randomOf = (seed: number) => () => {
  seed = (Math.imul(seed, 1_664_525) + 1_013_904_223) >>> 0;
  return seed / 0x1_0000_0000;
};
const changes = (rows: SqliteRow[]) => rows.map(row => ({
  commit: Number(row.commit_no), key: String(row.key), version: Number(row.version), deleted: Number(row.deleted) === 1,
}));

afterEach(() => {
  for (const handle of handles.splice(0).reverse()) handle.close();
  for (const where of roots.splice(0)) fs.rmSync(where, { recursive: true, force: true });
});

describe("SQLite feed READ queries", () => {
  it("seeks the tail after a backwards LIMIT-1 boundary, and stops oldest at the first row", async () => {
    const { db, since, oldest } = await captured();
    const plan = db.prepare(`EXPLAIN QUERY PLAN ${since}`).all(4095).map(row => String(row.detail));
    expect(plan.some(detail => /SEARCH changes USING INTEGER PRIMARY KEY \(rowid>\?\)/.test(detail))).toBe(true);
    expect(plan.some(detail => /TEMP B-TREE/.test(detail))).toBe(false);
    const code = db.prepare(`EXPLAIN ${since}`).all(4095);
    const last = code.find(row => row.opcode === "Last");
    const seek = code.find(row => row.opcode === "SeekGT");
    expect(last).toBeDefined();
    expect(seek).toBeDefined();
    expect(code.filter(row => row.opcode === "Prev").map(row => row.p1)).toEqual([last!.p1]);
    expect(code.filter(row => row.opcode === "Next").map(row => row.p1)).toEqual([seek!.p1]);
    expect(code.some(row => row.opcode === "Rewind" || row.opcode === "AggStep" || row.opcode === "SorterOpen")).toBe(false);
    const boundaryLimit = code.find(row => row.opcode === "DecrJumpZero");
    expect(boundaryLimit).toBeDefined();
    expect(code.some(row => row.opcode === "Integer" && row.p1 === 1 && row.p2 === boundaryLimit!.p1)).toBe(true);
    expect(Number(boundaryLimit!.addr)).toBeLessThan(Number(code.find(row => row.opcode === "Prev")!.addr));
    expect(Number(boundaryLimit!.p2)).toBeGreaterThan(Number(code.find(row => row.opcode === "Prev")!.addr));
    // Thus the reverse cursor visits only the k newer rows plus one boundary; the forward cursor
    // seeks past that boundary and visits k rows. No retained-prefix loop or temp sort is possible.
    const first = db.prepare(`EXPLAIN ${oldest}`).all();
    const next = first.find(row => row.opcode === "Next");
    const limit = first.find(row => row.opcode === "DecrJumpZero");
    expect(first.some(row => row.opcode === "AggStep" || row.opcode === "SorterOpen")).toBe(false);
    expect(first.some(row => row.opcode === "Rewind")).toBe(true);
    expect(limit).toBeDefined();
    expect(first.some(row => row.opcode === "Integer" && row.p1 === 1 && row.p2 === limit!.p1)).toBe(true);
    expect(Number(limit!.addr)).toBeLessThan(Number(next!.addr));
    expect(Number(limit!.p2)).toBeGreaterThan(Number(next!.addr));
    // Negative control: SQLite's legacy min(commit_no) is an aggregate over every retained row.
    expect(db.prepare(`EXPLAIN ${LEGACY_OLDEST}`).all().some(row => row.opcode === "AggStep")).toBe(true);
    expect(db.prepare(`EXPLAIN ${LEGACY_SINCE}`).all(4095).some(row => row.opcode === "SeekGT")).toBe(false);
  });

  it("matches legacy results on generated monotone feeds with gaps, trimmed starts and same-commit keys", async () => {
    const { db, since, oldest } = await captured();
    const insert = db.prepare("INSERT INTO changes(seq, commit_no, key, version, deleted) VALUES (?, ?, ?, ?, ?)");
    const actual = db.prepare(since);
    const legacy = db.prepare(LEGACY_SINCE);
    const random = randomOf(16879);
    for (let fixture = 0; fixture < 60; fixture++) {
      db.exec("DELETE FROM changes; BEGIN");
      let seq = fixture * 1000;
      let commit = fixture;
      const boundaries = new Set([-1, 0, commit, Infinity, -Infinity, NaN]);
      const count = fixture === 0 ? 0 : 1 + Math.floor(random() * 90);
      for (let index = 0; index < count; index++) {
        seq += 1 + Math.floor(random() * 4);
        commit += Math.floor(random() * 3); // Equal commits and skipped commits are both valid.
        insert.run(seq, commit, `k/${index % 9}`, index + 1, index % 3 === 0 ? 1 : 0);
        boundaries.add(commit - 1); boundaries.add(commit); boundaries.add(commit + 0.5);
      }
      db.exec("COMMIT");
      expect(db.prepare(oldest).get()?.oldest ?? null).toEqual(db.prepare(LEGACY_OLDEST).get()!.oldest);
      for (const after of [...boundaries, commit + 1]) expect(actual.all(after), `fixture ${fixture}, after ${after}`).toEqual(legacy.all(after));
    }
    // Counterexample to treating a commit as a rowid, or guessing a tail length from commit distance:
    // seq is not commit_no, and one commit can hold arbitrarily many keys (including deletions).
    db.exec("DELETE FROM changes");
    insert.run(100, 7, "old", 1, 0);
    for (let seq = 101; seq <= 130; seq++) insert.run(seq, 8, `new/${seq}`, seq, seq % 2);
    expect(actual.all(7)).toEqual(legacy.all(7));
    expect(actual.all(7)).toHaveLength(30);
    expect(db.prepare("SELECT commit_no, key, version, deleted FROM changes WHERE seq > ? ORDER BY seq").all(7)).not.toEqual(legacy.all(7));
    expect(db.prepare("SELECT commit_no, key, version, deleted FROM changes ORDER BY seq DESC LIMIT ?").all(8 - 7)).not.toEqual(legacy.all(7));
  });

  it("preserves public changesSince/readDelta after import, real batches, deletion, trimming and reopen", async () => {
    const where = root();
    let store = await open(where, { changesRetained: 8, maxStateTombstones: 3 });
    await store.importState({ entries: {
      "topology/a": { key: "topology/a", value: "import", version: 10, updatedAt: 1, updatedBy: identity },
      "other/b": { key: "other/b", value: "import", version: 11, updatedAt: 1, updatedBy: identity },
    }, highWater: 11 });
    const db = raw(where);
    const check = () => {
      const commit = store.exportState().commit;
      const oldest = db.prepare(LEGACY_OLDEST).get()!.oldest;
      for (const after of [-1, 0, 1, Math.max(0, commit - 3), commit - 1, commit, commit + 1]) {
        const complete = after >= commit || (oldest !== null && Number(oldest) <= after + 1);
        const rows = complete ? db.prepare(LEGACY_SINCE).all(after) : [];
        expect(store.changesSince(after)).toEqual({ commit, complete, changes: changes(rows) });
        const deltaComplete = after <= commit && complete;
        const changed = deltaComplete ? [...new Set(rows.map(row => String(row.key)))].map(key => {
          const entry = store.get(key);
          return { key, entry, version: entry?.version ?? Number(db.prepare("SELECT version FROM tombstones WHERE key = ?").get(key)?.version ?? 0) };
        }) : [];
        expect(store.readDelta(after, () => -1)).toMatchObject({ commit, complete: deltaComplete, changed });
      }
    };
    check();
    const random = randomOf(6477);
    for (let step = 0; step < 100; step++) {
      const ops: MeshBatchOperation[] = Array.from({ length: 1 + Math.floor(random() * 5) }, () => {
        const key = `topology/${Math.floor(random() * 8)}`;
        return random() < 0.35 ? { kind: "delete", key } : { kind: "put", key, value: step };
      });
      await store.writeBatch({ identity, ops });
      check();
    }
    expect(Number(db.prepare("SELECT min(seq) AS seq FROM changes").get()!.seq)).toBeGreaterThan(1);
    // A batch larger than retention is trimmed whole, even if that empties the feed. The next
    // append can restart seq at 1 while commit_no continues, so no cached boundary is safe.
    await store.writeBatch({ identity, ops: Array.from({ length: 70 }, (_, i) => ({ kind: "put", key: `large/${i}`, value: i })) });
    expect(db.prepare("SELECT count(*) AS n FROM changes").get()!.n).toBe(0);
    check();
    store.close();
    store = await open(where, { changesRetained: 8, maxStateTombstones: 3 });
    await store.put({ key: "resumed/a", value: 1, identity });
    expect(db.prepare("SELECT min(seq) AS seq FROM changes").get()!.seq).toBe(1);
    check();
  });

  it("keeps empty/import-only gaps conservative and prefix proofs correct through unrelated and relevant writes", async () => {
    const where = root();
    const store = await open(where, { changesRetained: 1 });
    expect(store.changesSince(0)).toEqual({ commit: 0, complete: true, changes: [] });
    await store.importState({ entries: {}, versions: { "gone/a": 9 }, highWater: 9 });
    expect(store.changesSince(0)).toEqual({ commit: 1, complete: false, changes: [] });
    expect(store.readDelta(0, () => 0)).toMatchObject({ commit: 1, complete: false, changed: [] });
    await store.put({ key: "unrelated/a", value: 0, identity });
    expect(store.changesSince(0).complete).toBe(false);
    expect(store.changesSince(1).complete).toBe(true);
    const mesh = new MeshStore(where, 64 * 1024, 1_000, { stateBackend: "sqlite" });
    handles.push({ close: () => mesh.closeState() });
    const stamp = mesh.stateRevision()!;
    await store.put({ key: "unrelated/b", value: 1, identity });
    expect(mesh.stateUnchangedSince(stamp, ["topology/"])).toBe(true);
    const beforePut = mesh.stateRevision()!;
    await store.put({ key: "topology/a", value: 2, identity });
    expect(mesh.stateUnchangedSince(beforePut, ["topology/"])).toBe(false);
    const beforeDelete = mesh.stateRevision()!;
    await store.delete({ key: "topology/a" });
    expect(mesh.stateUnchangedSince(beforeDelete, ["topology/"])).toBe(false);
    expect(mesh.stateUnchangedSince(beforeDelete.replace(/:[^:]+$/, ":999999"), ["topology/"])).toBe(true); // Existing ahead-stamp contract.
    for (let i = 0; i < 80; i++) await store.put({ key: "unrelated/a", value: i, identity });
    expect(mesh.stateUnchangedSince(stamp, ["topology/"])).toBe(false); // Trimmed proof cannot be accepted.
    expect(mesh.stateUnchangedSince("different:epoch:0", ["topology/"])).toBe(false);
  });
});
