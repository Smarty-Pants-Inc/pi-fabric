import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { STATE_PROJECTOR_STATUS_FILE, StateProjector, type StateProjectorEvent, type StateProjectorOptions } from "../src/mesh/state-projector.js";
import { openNodeSqlite, SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// Lane L3 of smarty-dev#6477: the projector keeps <root>/state.db in step with state.json.
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

const openStore = async (root: string): Promise<SqliteStateStore> => {
  const store = await SqliteStateStore.open(root, 64 * 1024, 1_000);
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
    const raw = openNodeSqlite(path.join(root, "state.db"));
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

  it("keeps reporting a divergence when repair is off", async () => {
    const root = tempRoot("no-repair");
    const file = new MeshStore(root, 64 * 1024, 1_000);
    await file.put({ key: "k/a", value: 1, identity });
    const projector = await openProjector(root, { repairDivergence: false });
    await projector.tick();
    const raw = openNodeSqlite(path.join(root, "state.db"));
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
    const raw = openNodeSqlite(path.join(root, "state.db"));
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
    const raw = openNodeSqlite(path.join(root, "state.db"));
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
    const authority = await openStore(root);
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
});
