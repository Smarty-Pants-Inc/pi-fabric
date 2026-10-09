import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { projectorDatabaseRoot, STATE_PROJECTOR_STATUS_FILE, StateProjector, type StateProjectorEvent, type StateProjectorOptions } from "../src/mesh/state-projector.js";
import { openNodeSqlite, SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// Lane L3 of smarty-dev#6477: the projector keeps <root>/state-projector/state.db in step with state.json
// (W1: <root>/state.db is the cutover flag, which fences file-mode writes).
const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
const projectors: StateProjector[] = [];
const stores: SqliteStateStore[] = [];

const tempRoot = (label: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-projector-${label}-`));
  roots.push(root);
  return root;
};

const openProjector = async (root: string, options: Partial<StateProjectorOptions> = {}): Promise<StateProjector> => {
  const projector = await StateProjector.open({ root, verifyMs: 0, statusMs: 0, ...options });
  projectors.push(projector);
  return projector;
};

const openStore = async (root: string, databaseRoot = projectorDatabaseRoot(root)): Promise<SqliteStateStore> => {
  const store = await SqliteStateStore.open(databaseRoot, 64 * 1024, 1_000);
  stores.push(store);
  return store;
};

afterEach(async () => {
  for (const projector of projectors.splice(0)) await projector.stop();
  for (const store of stores.splice(0)) try { store.close(); } catch { /* closed by the test */ }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

interface FileState { entries: Record<string, unknown>; versions: Record<string, number>; tombstoneOrder: string[]; highWater: number }
const fileState = (root: string): FileState => {
  const parsed = JSON.parse(fs.readFileSync(path.join(root, "state.json"), "utf8")) as Partial<FileState>;
  return { entries: parsed.entries ?? {}, versions: parsed.versions ?? {}, tombstoneOrder: parsed.tombstoneOrder ?? [], highWater: parsed.highWater ?? 0 };
};

/** The SQLite projection, read through the L1 store, equals state.json revision for revision. */
const expectInStep = async (root: string): Promise<void> => {
  const file = fileState(root);
  const store = await openStore(root);
  const projected = store.exportState();
  expect(projected.entries).toEqual(file.entries);
  expect(projected.versions).toEqual(file.versions);
  expect(projected.tombstoneOrder).toEqual(file.tombstoneOrder);
  expect(projected.highWater).toBe(file.highWater);
};

// A mix of puts, deletes (tombstone eviction at 3) and batches: one file generation each.
const writeGenerations = async (store: MeshStore, count: number, offset = 0): Promise<void> => {
  for (let step = offset; step < offset + count; step += 1) {
    if (step % 4 === 3) await store.delete({ key: `k/${step - 2}` });
    else if (step % 4 === 2) {
      await store.writeBatch({ identity, ops: [
        { kind: "put", key: `k/${step}`, value: { step, nested: [step, "x"] } },
        { kind: "put", key: "counter", value: step },
      ] });
    } else await store.put({ key: `k/${step}`, value: `value-${step}`, identity });
  }
};

describe("state projector (shadow)", () => {
  it("applies N generations from the journal, one SQLite commit each, readable through the L1 store", async () => {
    const root = tempRoot("apply");
    const file = new MeshStore(root, 64 * 1024, 1_000, { maxStateTombstones: 3 });
    await file.put({ key: "k/base", value: 0, identity });
    const projector = await openProjector(root);
    let status = await projector.tick();
    expect(status).toMatchObject({ role: "active", fullResyncs: 1, appliedGenerations: 0, lastResync: { reason: "initial", entries: 1 } });
    const firstCommit = status.commit;

    const generations = 16;
    await writeGenerations(file, generations);
    const lagSeen: number[] = [];
    await projector.stop();
    const follower = await openProjector(root, { owner: "follower", beforeCommit: () => { lagSeen.push(follower.status().lag.generations); } });
    status = await follower.tick();
    expect(status).toMatchObject({ role: "active", fullResyncs: 0, appliedGenerations: generations, lag: { revisions: 0, generations: 0, ms: 0 } });
    expect(status.commit).toBe(firstCommit + generations);
    expect(status.lastApplyLagMs).toBeGreaterThanOrEqual(0);
    // Lag was recorded before each apply: all N pending at the first, one at the last.
    expect(lagSeen[0]).toBe(generations);
    expect(lagSeen.at(-1)).toBe(1);
    await expectInStep(root);

    const reader = await openStore(root);
    expect(reader.get("counter")).toEqual(file.get("counter"));
    expect(reader.listAll("k/")).toEqual(file.listAll("k/"));
    expect(reader.stateStamp()).toMatch(new RegExp(`:${status.commit}$`));
    const feed = reader.changesSince(firstCommit);
    expect(feed.complete).toBe(true);
    expect(new Set(feed.changes.map(change => change.commit)).size).toBe(generations);

    // Idempotent: a restarted projector (or a repeated pass) applies nothing and commits nothing.
    status = await follower.tick();
    expect(status).toMatchObject({ appliedGenerations: generations, fullResyncs: 0, commit: firstCommit + generations });
    await follower.stop();
    const again = await openProjector(root, { owner: "again" });
    status = await again.tick();
    expect(status).toMatchObject({ role: "active", appliedGenerations: 0, fullResyncs: 0, commit: firstCommit + generations });
  });

  it("follows two concurrent writers and converges", async () => {
    const root = tempRoot("concurrent");
    const left = new MeshStore(root, 64 * 1024, 1_000, { maxStateTombstones: 5 });
    const right = new MeshStore(root, 64 * 1024, 1_000, { maxStateTombstones: 5 });
    await left.put({ key: "k/base", value: 0, identity });
    const projector = await openProjector(root, { pollMs: 5 });
    await projector.tick();
    projector.run();
    // Each writer yields a timer turn between commits, so the loop interleaves with them.
    const writer = async (store: MeshStore, offset: number): Promise<void> => {
      for (let step = 0; step < 40; step += 1) {
        await writeGenerations(store, 1, offset + step);
        await new Promise(resolve => setTimeout(resolve, 1));
      }
    };
    await Promise.all([writer(left, 0), writer(right, 100)]);
    const target = fileState(root);
    const sorted = (entries: Record<string, unknown>) => JSON.stringify(Object.keys(entries).sort().map(key => entries[key]));
    const deadline = Date.now() + 10_000;
    for (;;) {
      const status = await projector.tick();
      if (status.lag.generations === 0) {
        const store = await openStore(root);
        const done = sorted(store.exportState().entries) === sorted(target.entries);
        store.close();
        if (done) break;
      }
      if (Date.now() > deadline) throw new Error("projector did not catch up");
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    expect(await projector.verify()).toEqual([]);
    await expectInStep(root);
    const status = projector.status();
    // Every one of the 80 generations went through the journal or a resync; none was skipped.
    expect(status.appliedGenerations).toBeGreaterThan(0);
    expect(status.errors).toBe(0);
    expect(status.divergences).toBe(0);
    if (process.env.PROJECTOR_PROBE) fs.appendFileSync(process.env.PROJECTOR_PROBE, `concurrent: applied ${status.appliedGenerations} generations, ${status.fullResyncs} full resyncs, maxLagMs ${status.maxLagMs}\n`);
  });

  it("falls back to a full resync after a journal gap, then follows the journal again", async () => {
    const root = tempRoot("gap");
    const file = new MeshStore(root, 64 * 1024, 1_000, { maxStateTombstones: 3 });
    await file.put({ key: "k/base", value: 0, identity });
    const projector = await openProjector(root);
    await projector.tick();

    // A legacy writer publishes no journal record and no chain hash.
    const legacy = new MeshStore(root, 64 * 1024, 1_000, { writeReadJournal: false, maxStateTombstones: 3 });
    await legacy.put({ key: "legacy/1", value: "old binary", identity });
    let status = await projector.tick();
    expect(status).toMatchObject({ fullResyncs: 2, lastResync: { reason: "gap" } });
    await expectInStep(root);

    // Journal-publishing writers resume: incremental again.
    await writeGenerations(file, 5);
    status = await projector.tick();
    expect(status).toMatchObject({ fullResyncs: 2, appliedGenerations: 5 });
    await expectInStep(root);

    // A lost record (journal removed between two commits) breaks the chain: resync.
    await file.put({ key: "k/lost", value: 1, identity });
    fs.rmSync(path.join(root, "state.read-journal.jsonl"));
    await file.put({ key: "k/after", value: 2, identity });
    status = await projector.tick();
    expect(status).toMatchObject({ fullResyncs: 3, appliedGenerations: 5, lastResync: { reason: "gap" } });
    await expectInStep(root);
    await file.delete({ key: "k/lost" });
    status = await projector.tick();
    expect(status).toMatchObject({ fullResyncs: 3, appliedGenerations: 6 });
    await expectInStep(root);
  });

  it("detects SQLite edited behind its back, records the divergence and repairs it", async () => {
    const root = tempRoot("divergence");
    const file = new MeshStore(root, 64 * 1024, 1_000, { maxStateTombstones: 3 });
    await writeGenerations(file, 8);
    const events: StateProjectorEvent[] = [];
    const projector = await openProjector(root, { statusMs: 1, onEvent: (event) => { events.push(event); } });
    await projector.tick();
    expect(await projector.verify()).toEqual([]);

    // Edits that bypass the projector and every commit counter.
    const raw = openNodeSqlite(path.join(projectorDatabaseRoot(root), "state.db"));
    try {
      raw.prepare("UPDATE kv SET value = ? WHERE key = ?").run(JSON.stringify("tampered"), "k/0");
      raw.prepare("DELETE FROM kv WHERE key = ?").run("k/4");
      raw.prepare("UPDATE tombstones SET version = version + 100 WHERE key = ?").run("k/1");
    } finally { raw.close(); }

    const found = await projector.verify();
    expect(found.map(divergence => [divergence.key, divergence.field]).sort()).toEqual([["k/0", "value"], ["k/1", "tombstone"], ["k/4", "presence"]]);
    const tampered = found.find(divergence => divergence.key === "k/0")!;
    expect(tampered.file).toEqual({ value: "value-0", updatedBy: identity });
    expect(tampered.sqlite).toEqual({ value: "tampered", updatedBy: identity });
    const status = projector.status();
    expect(status).toMatchObject({ divergences: 3, divergenceChecks: 2, fullResyncs: 2, lastResync: { reason: "divergence" } });
    expect(status.divergentKeys.sort()).toEqual(["k/0", "k/1", "k/4"]);
    expect(events.some(event => event.type === "divergence" && event.divergences.length === 3)).toBe(true);
    // Visible to commswatch in the status file.
    const published = JSON.parse(fs.readFileSync(path.join(root, STATE_PROJECTOR_STATUS_FILE), "utf8")) as { divergences: number };
    expect(published.divergences).toBe(3);
    // Repaired.
    expect(await projector.verify()).toEqual([]);
    await expectInStep(root);
  });

  it("detects tombstones with the file's keys and versions in a different ord order and repairs it", async () => {
    const root = tempRoot("tombstone-order");
    const file = new MeshStore(root, 64 * 1024, 1_000, { maxStateTombstones: 5 });
    for (const key of ["t/a", "t/b", "t/c"]) await file.put({ key, value: key, identity });
    for (const key of ["t/a", "t/b", "t/c"]) await file.delete({ key });
    expect(fileState(root).tombstoneOrder).toEqual(["t/a", "t/b", "t/c"]);
    const projector = await openProjector(root);
    await projector.tick();
    expect(await projector.verify()).toEqual([]);

    // Same keys, same versions; only the ords of t/a and t/c are swapped (ord is UNIQUE: via a free slot).
    const raw = openNodeSqlite(path.join(projectorDatabaseRoot(root), "state.db"));
    let before: Array<{ key: string; version: number; ord: number }>;
    try {
      before = raw.prepare("SELECT key, version, ord FROM tombstones ORDER BY ord").all() as typeof before;
      const ordOf = (key: string) => before.find(row => row.key === key)!.ord;
      const [a, c] = [ordOf("t/a"), ordOf("t/c")];
      raw.prepare("UPDATE tombstones SET ord = ? WHERE key = ?").run(-1, "t/a");
      raw.prepare("UPDATE tombstones SET ord = ? WHERE key = ?").run(a, "t/c");
      raw.prepare("UPDATE tombstones SET ord = ? WHERE key = ?").run(c, "t/a");
      const swapped = raw.prepare("SELECT key, version FROM tombstones ORDER BY ord").all() as Array<{ key: string; version: number }>;
      expect(swapped.map(row => row.key)).toEqual(["t/c", "t/b", "t/a"]);
      // An unordered comparison would see no difference.
      expect(new Map(swapped.map(row => [row.key, row.version]))).toEqual(new Map(before.map(row => [row.key, row.version])));
    } finally { raw.close(); }

    const found = await projector.verify();
    expect(found.map(divergence => [divergence.key, divergence.field])).toEqual([["t/c", "tombstone-order"], ["t/a", "tombstone-order"]]);
    expect(found[0]).toMatchObject({ fileVersion: found[0]!.sqliteVersion, filePosition: 2, sqlitePosition: 0 });
    expect(found[1]).toMatchObject({ fileVersion: found[1]!.sqliteVersion, filePosition: 0, sqlitePosition: 2 });
    expect(projector.status()).toMatchObject({ divergences: 2, fullResyncs: 2, lastResync: { reason: "divergence" } });
    // Repaired by the resync: a second verify is clean and SQLite has the file's order again.
    expect(await projector.verify()).toEqual([]);
    await expectInStep(root);
  });

  it("reports a tombstone ord above the tombstone_ord mark as a tombstone-order divergence", async () => {
    const root = tempRoot("tombstone-ord-mark");
    const file = new MeshStore(root, 64 * 1024, 1_000);
    await file.put({ key: "t/a", value: 1, identity });
    await file.delete({ key: "t/a" });
    const projector = await openProjector(root);
    await projector.tick();
    expect(await projector.verify()).toEqual([]);
    const raw = openNodeSqlite(path.join(projectorDatabaseRoot(root), "state.db"));
    try { raw.prepare("UPDATE tombstones SET ord = ord + 1000 WHERE key = ?").run("t/a"); } finally { raw.close(); }
    expect((await projector.verify()).map(divergence => [divergence.key, divergence.field])).toEqual([["t/a", "tombstone-order"]]);
    expect(await projector.verify()).toEqual([]);
  });

  it("reports out-of-band byte accounting (row bytes, meta.state_bytes) without a commit and repairs it", async () => {
    const root = tempRoot("bytes");
    const file = new MeshStore(root, 64 * 1024, 1_000);
    await file.put({ key: "k/a", value: "file-a", identity });
    await file.put({ key: "k/b", value: "file-b", identity });
    await file.delete({ key: "k/b" });
    const projector = await openProjector(root);
    await projector.tick();
    expect(await projector.verify()).toEqual([]);
    const database = path.join(projectorDatabaseRoot(root), "state.db");
    const read = () => {
      const raw = openNodeSqlite(database);
      try {
        return {
          bytes: Number((raw.prepare("SELECT bytes FROM kv WHERE key = ?").get("k/a") as { bytes: number }).bytes),
          stateBytes: Number((raw.prepare("SELECT value FROM meta WHERE name = 'state_bytes'").get() as { value: number }).value),
          commit: Number((raw.prepare("SELECT value FROM meta WHERE name = 'commit_no'").get() as { value: number }).value),
        };
      } finally { raw.close(); }
    };
    const clean = read();
    const raw = openNodeSqlite(database);
    try {
      raw.prepare("UPDATE kv SET bytes = 1 WHERE key = ?").run("k/a");
      raw.prepare("UPDATE meta SET value = 7 WHERE name = 'state_bytes'").run();
    } finally { raw.close(); }
    expect(read().commit).toBe(clean.commit);

    const found = await projector.verify();
    expect(found.map(divergence => [divergence.key, divergence.field])).toEqual([["k/a", "bytes"], ["meta:state_bytes", "state-bytes"]]);
    expect(found[0]).toMatchObject({ fileBytes: clean.bytes, sqliteBytes: 1 });
    expect(found[1]).toMatchObject({ fileBytes: clean.stateBytes, sqliteBytes: 7 });
    expect(projector.status()).toMatchObject({ fullResyncs: 2, lastResync: { reason: "divergence" } });
    // Repaired: the row's bytes and the aggregate are the derived accounting again.
    expect(read()).toMatchObject({ bytes: clean.bytes, stateBytes: clean.stateBytes });
    expect(await projector.verify()).toEqual([]);
    await expectInStep(root);
  });

  it("reports meta.high_water lowered out of band without a commit and raises it back", async () => {
    const root = tempRoot("high-water");
    const file = new MeshStore(root, 64 * 1024, 1_000);
    await file.put({ key: "k/a", value: 1, identity });
    await file.put({ key: "k/b", value: 2, identity });
    const projector = await openProjector(root);
    await projector.tick();
    expect(await projector.verify()).toEqual([]);
    const fileHighWater = fileState(root).highWater;
    expect(fileHighWater).toBeGreaterThan(1);
    const database = path.join(projectorDatabaseRoot(root), "state.db");
    const meta = (name: string): number => {
      const raw = openNodeSqlite(database);
      try { return Number((raw.prepare("SELECT value FROM meta WHERE name = ?").get(name) as { value: number }).value); } finally { raw.close(); }
    };
    const commit = meta("commit_no");
    const raw = openNodeSqlite(database);
    try { raw.prepare("UPDATE meta SET value = 0 WHERE name = 'high_water'").run(); } finally { raw.close(); }
    expect(meta("commit_no")).toBe(commit);

    const found = await projector.verify();
    expect(found.map(divergence => [divergence.key, divergence.field])).toEqual([["meta:high_water", "high-water"]]);
    expect(found[0]).toMatchObject({ fileVersion: fileHighWater, sqliteVersion: 0 });
    expect(meta("high_water")).toBe(fileHighWater);
    expect(await projector.verify()).toEqual([]);
    await expectInStep(root);
  });

  it("publishes a same-version value or metadata repair as a commit that stamp and change-feed readers see", async () => {
    const root = tempRoot("repair-commit");
    const file = new MeshStore(root, 64 * 1024, 1_000);
    await file.put({ key: "k/a", value: "file-a", identity });
    await file.put({ key: "k/b", value: "file-b", identity });
    const projector = await openProjector(root);
    await projector.tick();
    const reader = await openStore(root);
    const base = projector.status().commit;
    // A clean verify commits nothing.
    expect(await projector.verify()).toEqual([]);
    expect(projector.status().commit).toBe(base);
    expect(reader.stateStamp()).toMatch(new RegExp(`:${base}$`));

    // Same versions, different value (k/a) and metadata (k/b): no commit counter moves.
    const raw = openNodeSqlite(path.join(projectorDatabaseRoot(root), "state.db"));
    try {
      raw.prepare("UPDATE kv SET value = ? WHERE key = ?").run(JSON.stringify("tampered"), "k/a");
      raw.prepare("UPDATE kv SET updated_by = ? WHERE key = ?").run(JSON.stringify({ id: "other", name: "other", kind: "agent" }), "k/b");
    } finally { raw.close(); }
    // A stamp reader caches what it reads under the stamp it read it at.
    const stamp = reader.stateStamp();
    expect(reader.get("k/a")?.value).toBe("tampered");

    const found = await projector.verify();
    expect(found.map(divergence => [divergence.key, divergence.field]).sort()).toEqual([["k/a", "value"], ["k/b", "updatedBy"]]);
    expect(projector.status().commit).toBe(base + 1);
    // The stamp moved, so the cached "tampered" is invalid; a re-read serves the repaired row.
    expect(reader.stateStamp()).not.toBe(stamp);
    expect(reader.stateStamp()).toMatch(new RegExp(`:${base + 1}$`));
    expect(reader.get("k/a")).toEqual(file.get("k/a"));
    expect(reader.get("k/b")).toEqual(file.get("k/b"));
    const feed = reader.changesSince(base);
    expect(feed).toMatchObject({ commit: base + 1, complete: true });
    expect(feed.changes.map(change => [change.commit, change.key, change.version, change.deleted]).sort()).toEqual([
      [base + 1, "k/a", file.get("k/a")!.version, false],
      [base + 1, "k/b", file.get("k/b")!.version, false],
    ]);
    // Repaired: the next verify is clean and commits nothing.
    expect(await projector.verify()).toEqual([]);
    expect(projector.status().commit).toBe(base + 1);
    await expectInStep(root);
  });

  it("publishes a tombstone-order-only repair as a commit; a clean verify leaves commit_no", async () => {
    const root = tempRoot("repair-order-commit");
    const file = new MeshStore(root, 64 * 1024, 1_000, { maxStateTombstones: 5 });
    for (const key of ["t/a", "t/b", "t/c"]) await file.put({ key, value: key, identity });
    for (const key of ["t/a", "t/b", "t/c"]) await file.delete({ key });
    const projector = await openProjector(root);
    await projector.tick();
    const reader = await openStore(root);
    const base = projector.status().commit;
    expect(await projector.verify()).toEqual([]);
    expect(projector.status().commit).toBe(base);

    // Swap the ords of t/a and t/c: same keys, same versions, only the order differs.
    const raw = openNodeSqlite(path.join(projectorDatabaseRoot(root), "state.db"));
    try {
      const rows = raw.prepare("SELECT key, ord FROM tombstones").all() as Array<{ key: string; ord: number }>;
      const ordOf = (key: string) => rows.find(row => row.key === key)!.ord;
      const [a, c] = [ordOf("t/a"), ordOf("t/c")];
      raw.prepare("UPDATE tombstones SET ord = ? WHERE key = ?").run(-1, "t/a");
      raw.prepare("UPDATE tombstones SET ord = ? WHERE key = ?").run(a, "t/c");
      raw.prepare("UPDATE tombstones SET ord = ? WHERE key = ?").run(c, "t/a");
    } finally { raw.close(); }
    const stamp = reader.stateStamp();
    expect(reader.exportState().tombstoneOrder).toEqual(["t/c", "t/b", "t/a"]);

    expect((await projector.verify()).map(divergence => divergence.field)).toEqual(["tombstone-order", "tombstone-order"]);
    expect(projector.status().commit).toBe(base + 1);
    expect(reader.stateStamp()).not.toBe(stamp);
    expect(reader.exportState().tombstoneOrder).toEqual(["t/a", "t/b", "t/c"]);
    const feed = reader.changesSince(base);
    expect(feed.complete).toBe(true);
    expect(new Set(feed.changes.map(change => change.commit))).toEqual(new Set([base + 1]));
    expect(feed.changes.every(change => change.deleted)).toBe(true);
    expect(feed.changes.map(change => change.key)).toEqual(expect.arrayContaining(["t/a", "t/c"]));

    expect(await projector.verify()).toEqual([]);
    expect(projector.status().commit).toBe(base + 1);
    await expectInStep(root);
  });

  it("keeps reporting a divergence when repair is off", async () => {
    const root = tempRoot("no-repair");
    const file = new MeshStore(root, 64 * 1024, 1_000);
    await file.put({ key: "k/a", value: 1, identity });
    const projector = await openProjector(root, { repairDivergence: false });
    await projector.tick();
    const raw = openNodeSqlite(path.join(projectorDatabaseRoot(root), "state.db"));
    try { raw.prepare("UPDATE kv SET updated_at = 1 WHERE key = ?").run("k/a"); } finally { raw.close(); }
    expect((await projector.verify()).map(divergence => divergence.field)).toEqual(["updatedAt"]);
    expect((await projector.verify()).map(divergence => divergence.field)).toEqual(["updatedAt"]);
    expect(projector.status()).toMatchObject({ divergences: 2, fullResyncs: 1 });
  });

  it("stops with an alarm on a foreign write and never overwrites it", async () => {
    const root = tempRoot("foreign");
    const file = new MeshStore(root, 64 * 1024, 1_000);
    await file.put({ key: "k/a", value: "file", identity });
    const events: StateProjectorEvent[] = [];
    const projector = await openProjector(root, { onEvent: (event) => { events.push(event); } });
    await projector.tick();
    // A cut-over sqlite writer commits to the same database.
    const writer = await openStore(root);
    await writer.put({ key: "k/a", value: "sqlite", identity });
    await file.put({ key: "k/a", value: "file again", identity });
    const status = await projector.tick();
    expect(status).toMatchObject({ role: "stopped", haltReason: "foreign-write", foreignWrites: 1 });
    expect(events).toContainEqual(expect.objectContaining({ type: "alarm", alarm: "foreign-write" }));
    expect(writer.get("k/a")?.value).toBe("sqlite");
    expect((await projector.tick()).role).toBe("stopped");
  });

  it("refuses a database it did not create", async () => {
    const root = tempRoot("foreign-db");
    const authority = await openStore(root);
    await authority.put({ key: "k/a", value: "authoritative", identity });
    const file = new MeshStore(root, 64 * 1024, 1_000);
    await file.put({ key: "k/a", value: "stale file", identity });
    const projector = await openProjector(root);
    expect(await projector.tick()).toMatchObject({ role: "stopped", haltReason: "foreign-database" });
    expect(authority.get("k/a")?.value).toBe("authoritative");
  });

  it("elects one active projector per root; the standby takes over and continues the chain", async () => {
    const root = tempRoot("election");
    const file = new MeshStore(root, 64 * 1024, 1_000, { maxStateTombstones: 3 });
    await file.put({ key: "k/base", value: 0, identity });
    const first = await openProjector(root, { owner: "first" });
    const second = await openProjector(root, { owner: "second" });
    const [a, b] = await Promise.all([first.tick(), second.tick()]);
    expect([a.role, b.role].sort()).toEqual(["active", "standby"]);
    const [active, standby] = a.role === "active" ? [first, second] : [second, first];
    expect(standby.status().leaseHolder).toBe(active.owner);

    for (let round = 0; round < 3; round += 1) {
      await writeGenerations(file, 3, round * 3);
      await Promise.all([first.tick(), second.tick()]);
    }
    expect(active.status()).toMatchObject({ role: "active", appliedGenerations: 9 });
    expect(standby.status()).toMatchObject({ role: "standby", appliedGenerations: 0, fullResyncs: 0 });
    await expectInStep(root);

    // A clean stop releases the lease: the standby takes over at once and follows the journal.
    await active.stop();
    await writeGenerations(file, 2, 9);
    expect(await standby.tick()).toMatchObject({ role: "active", appliedGenerations: 2, fullResyncs: 0 });
    await expectInStep(root);

    // Fencing: a projector whose lease was taken cannot commit.
    const raw = openNodeSqlite(path.join(projectorDatabaseRoot(root), "state.db"));
    try {
      raw.prepare("UPDATE meta SET value = ? WHERE name = 'projector.lease'").run(JSON.stringify({ owner: "intruder", expiresAt: Date.now() + 60_000 }));
    } finally { raw.close(); }
    await file.put({ key: "k/fenced", value: 1, identity });
    expect(await standby.tick()).toMatchObject({ role: "standby", leaseHolder: "intruder", appliedGenerations: 2 });
  });

  it("never takes the mesh .lock, even while another process holds it", async () => {
    const root = tempRoot("lock");
    const file = new MeshStore(root, 64 * 1024, 1_000);
    await writeGenerations(file, 4);
    const projector = await openProjector(root);
    await projector.tick();
    await writeGenerations(file, 4, 4);
    // A held mesh lock (as a writer leaves it during its commit).
    fs.mkdirSync(path.join(root, ".lock"));
    const seen: string[] = [];
    const watcher = fs.watch(root, (_event, name) => { if (name) seen.push(String(name)); });
    try {
      expect(await projector.tick()).toMatchObject({ appliedGenerations: 4 });
      expect(await projector.verify()).toEqual([]);
      await new Promise(resolve => setTimeout(resolve, 50));
    } finally { watcher.close(); }
    expect(seen.filter(name => name.startsWith(".lock"))).toEqual([]);
    fs.rmSync(path.join(root, ".lock"), { recursive: true, force: true });
    await expectInStep(root);
  });

  it("runs its loop, catches up on its own and stops cleanly", async () => {
    const root = tempRoot("loop");
    const file = new MeshStore(root, 64 * 1024, 1_000);
    await file.put({ key: "k/base", value: 0, identity });
    const projector = (await openProjector(root, { pollMs: 10, statusMs: 1 })).run();
    await writeGenerations(file, 6);
    const target = fileState(root);
    const deadline = Date.now() + 10_000;
    for (;;) {
      const store = await openStore(root);
      const projected = store.exportState();
      store.close();
      const sorted = (entries: Record<string, unknown>) => JSON.stringify(Object.keys(entries).sort().map(key => entries[key]));
      if (sorted(projected.entries) === sorted(target.entries)) break;
      if (Date.now() > deadline) throw new Error("projector did not catch up");
      await new Promise(resolve => setTimeout(resolve, 20));
    }
    await projector.stop();
    expect(projector.status().role).toBe("stopped");
    expect(projector.status().haltReason).toBeUndefined();
    const published = JSON.parse(fs.readFileSync(path.join(root, STATE_PROJECTOR_STATUS_FILE), "utf8")) as { role: string };
    expect(published.role).toBe("stopped");
    // Lease released, no pass after stop.
    const raw = openNodeSqlite(path.join(projectorDatabaseRoot(root), "state.db"));
    try { expect(raw.prepare("SELECT value FROM meta WHERE name = 'projector.lease'").get()).toBeUndefined(); }
    finally { raw.close(); }
    await file.put({ key: "k/after-stop", value: 1, identity });
    expect((await projector.tick()).role).toBe("stopped");
    await new Promise(resolve => setTimeout(resolve, 50));
    const store = await openStore(root);
    expect(store.get("k/after-stop")).toBeUndefined();
  });
});

describe("state projector (sqlite, after cutover)", () => {
  it("maintains the authoritative database without touching its rows", async () => {
    const root = tempRoot("maintain");
    const authority = await openStore(root, root);
    for (let step = 0; step < 20; step += 1) await authority.put({ key: `k/${step}`, value: step, identity });
    const before = authority.exportState();
    const projector = await openProjector(root, { mode: "sqlite", checkpointMs: 10 });
    expect(await projector.tick()).toMatchObject({ role: "active", checkpoints: 1, fullResyncs: 0, appliedGenerations: 0 });
    await authority.put({ key: "k/after", value: 1, identity });
    await new Promise(resolve => setTimeout(resolve, 15));
    expect(await projector.tick()).toMatchObject({ role: "active", checkpoints: 2 });
    expect(authority.exportState().entries).toEqual({ ...before.entries, "k/after": authority.get("k/after") });
    expect(fs.existsSync(path.join(root, "state.json"))).toBe(false);
  });

  it.each(["exporting", "file"])("halts retired when meta.backend leaves sqlite (%s): no renewal, no checkpoint (smarty-dev#7064)", async backend => {
    const root = tempRoot(`backend-${backend}`);
    const authority = await openStore(root, root);
    await authority.put({ key: "k/0", value: 0, identity });
    const events: StateProjectorEvent[] = [];
    // A short lease so the next tick would renew it, a short checkpoint period so it would checkpoint.
    const projector = await openProjector(root, { mode: "sqlite", checkpointMs: 10, leaseMs: 100, onEvent: event => events.push(event) });
    expect(await projector.tick()).toMatchObject({ role: "active", checkpoints: 1 });
    const database = path.join(root, "state.db");
    const leaseOf = (): string | undefined => {
      const raw = openNodeSqlite(database);
      try { return raw.prepare("SELECT value FROM meta WHERE name = 'projector.lease'").get()?.value as string | undefined; }
      finally { raw.close(); }
    };
    const lease = leaseOf();
    expect(lease).toBeDefined();
    // A rollback flips the flag while the projector runs.
    const raw = openNodeSqlite(database);
    try { raw.prepare("UPDATE meta SET value = ? WHERE name = 'backend'").run(backend); } finally { raw.close(); }
    await new Promise(resolve => setTimeout(resolve, 70));
    const status = await projector.tick();
    expect(status).toMatchObject({ role: "stopped", haltReason: "retired", checkpoints: 1 });
    expect(events.some(event => event.type === "alarm" && event.alarm === "retired")).toBe(true);
    // The lease is neither renewed nor rewritten: the retired database is left to the rollback.
    expect(leaseOf()).toBe(lease);
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(await projector.tick()).toMatchObject({ role: "stopped", haltReason: "retired", checkpoints: 1 });
    expect(leaseOf()).toBe(lease);
  });
});
