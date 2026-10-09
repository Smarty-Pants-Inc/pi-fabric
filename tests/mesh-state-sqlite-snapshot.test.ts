import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SqliteStateBackend } from "../src/mesh/state-backend.js";
import { SqliteStateStore, type SqliteStateExport } from "../src/mesh/state-sqlite.js";
import { MeshBatchConflictError, MeshStore, type MeshBatchOperation, type MeshIdentity, type MeshStateEntry,
  type MeshStoreOptions } from "../src/mesh/store.js";

// smarty-dev#6477: the SQLite backend's snapshot is brought forward by the change feed (only changed
// rows are read) and shared per state.db in the process; it must equal a full export at every step.

const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
const stores: MeshStore[] = [];
const connections: SqliteStateStore[] = [];

const tempRoot = (label: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-sqsnap-${label}-`));
  roots.push(root);
  return root;
};
const open = (root: string, options: MeshStoreOptions = {}): MeshStore => {
  const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "sqlite", ...options });
  stores.push(store);
  return store;
};
const connect = async (root: string, options: Parameters<typeof SqliteStateStore.open>[3] = {}): Promise<SqliteStateStore> => {
  const store = await SqliteStateStore.open(root, 64 * 1024, 1_000, options);
  connections.push(store);
  return store;
};
const sqliteOf = (store: MeshStore): SqliteStateBackend => {
  const backend = store.stateBackendHandle;
  if (!(backend instanceof SqliteStateBackend)) throw new Error(`not sqlite: ${backend.kind}`);
  return backend;
};
const prng = (seed: number) => () => {
  seed = (seed * 1_103_515_245 + 12_345) & 0x7fffffff;
  return seed / 0x7fffffff;
};

// The snapshot's internals (a token is the snapshot object itself).
interface Snapshot { stamp: string; commit: number; sorted: readonly MeshStateEntry[]; versions: ReadonlyMap<string, number> }
const snapshotOf = (store: MeshStore): Snapshot => store.stateToken() as unknown as Snapshot;

const expected = (state: SqliteStateExport) => ({
  sorted: Object.values(state.entries).sort((left, right) => left.key.localeCompare(right.key)),
  versions: new Map(Object.entries(state.versions)),
});
const expectEqualsExport = (store: MeshStore, state: SqliteStateExport): void => {
  const snapshot = snapshotOf(store);
  const want = expected(state);
  expect(snapshot.commit).toBe(state.commit);
  expect(snapshot.sorted.map((entry) => entry.key)).toEqual(want.sorted.map((entry) => entry.key));
  expect(snapshot.sorted).toEqual(want.sorted);
  expect(new Map(snapshot.versions)).toEqual(want.versions);
  expect(Object.isFrozen(snapshot.sorted)).toBe(true);
  expect(snapshot.sorted.every((entry) => Object.isFrozen(entry))).toBe(true);
  expect(store.listAllShared("")).toEqual(want.sorted);
  expect(store.list("k", 1_000)).toEqual(want.sorted.filter((entry) => entry.key.startsWith("k")));
};

// Keys that exercise localeCompare (case, punctuation, digits) rather than byte order.
const KEYS = ["k/a", "k/A", "k/b", "k/B-1", "k/b.1", "k/b-1", "k/b:1", "k/10", "k/9", "k/a/x", "k/a.x", "k-a", "k.a",
  "K/a", "Ka", "x/1", "x/2", "x/3", "y", "Y", "z9", "z10", "0", "9/a"];

beforeEach(() => { vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", ""); });
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const store of stores.splice(0)) try { store.closeState(); } catch { /* closed by the test */ }
  for (const store of connections.splice(0)) try { store.close(); } catch { /* closed */ }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("sqlite incremental snapshot", () => {
  it("equals a full export after random puts, deletes and tombstone evictions from two connections", async () => {
    const root = tempRoot("property");
    // Few retained tombstones: deletes evict, which writes no change row (the count check reloads them).
    const reader = open(root, { maxStateTombstones: 3 });
    await reader.put({ key: "seed", value: 0, identity });
    const foreign = await connect(root, { maxStateTombstones: 3 });
    const exports = vi.spyOn(SqliteStateStore.prototype, "exportLive");
    const deltas = vi.spyOn(SqliteStateStore.prototype, "readDelta");
    const random = prng(6477);
    const pick = (): string => KEYS[Math.floor(random() * KEYS.length)]!;
    const value = (step: number): unknown => random() < 0.3
      ? { step, expiresAt: Date.now() + Math.floor(random() * 10_000) - 5_000, nested: { list: [step, "x"] } }
      : random() < 0.5 ? `v${step}` : step;
    const pins: Array<{ token: object; state: SqliteStateExport }> = [];
    for (let step = 0; step < 300; step += 1) {
      const writer = random() < 0.5 ? "reader" : "foreign";
      const roll = random();
      if (roll < 0.45) {
        const key = pick();
        if (writer === "reader") await reader.put({ key, value: value(step), identity });
        else await foreign.put({ key, value: value(step), identity });
      } else if (roll < 0.75) {
        const key = pick();
        if (writer === "reader") await reader.delete({ key });
        else await foreign.delete({ key });
      } else {
        const ops: MeshBatchOperation[] = Array.from({ length: 1 + Math.floor(random() * 5) }, () => random() < 0.6
          ? { kind: "put", key: pick(), value: value(step) } : { kind: "delete", key: pick() });
        if (writer === "reader") await reader.writeBatch({ identity, ops });
        else await foreign.writeBatch({ identity, ops });
      }
      // Read after 0..2 further commits: one delta often spans several commits.
      if (random() < 0.45) continue;
      const state = foreign.exportState({ live: true });
      expectEqualsExport(reader, state);
      if (step % 40 === 0) pins.push({ token: reader.stateToken(), state });
    }
    // Every rebuild after the first was incremental, and old snapshots were never mutated.
    expect(exports.mock.calls.length).toBeLessThanOrEqual(1);
    expect(deltas.mock.calls.length).toBeGreaterThan(50);
    expect(pins.length).toBeGreaterThan(2);
    for (const { token, state } of pins) {
      for (const key of KEYS) expect(reader.get(key, { snapshot: token })).toEqual(state.entries[key]);
    }
  });

  it("falls back to a full export when the change feed was trimmed past the snapshot", async () => {
    const root = tempRoot("trimmed");
    const reader = open(root);
    await reader.put({ key: "k/a", value: 1, identity });
    expect(reader.listAllShared("").map((entry) => entry.value)).toEqual([1]);
    const base = snapshotOf(reader).commit;
    const foreign = await connect(root, { changesRetained: 1 });
    for (let index = 0; index < 150; index += 1) await foreign.put({ key: `x/${index % 7}`, value: index, identity });
    expect(foreign.changesSince(base).complete).toBe(false);
    const exports = vi.spyOn(SqliteStateStore.prototype, "exportLive");
    expectEqualsExport(reader, foreign.exportState({ live: true }));
    expect(exports).toHaveBeenCalledTimes(1);
    // Afterwards the feed covers the new snapshot again: the next foreign commit is a delta.
    await foreign.put({ key: "k/a", value: 2, identity });
    expectEqualsExport(reader, foreign.exportState({ live: true }));
    expect(exports).toHaveBeenCalledTimes(1);
  });

  it("shares one snapshot across stores of one state.db in a process", async () => {
    const root = tempRoot("shared");
    const a = open(root);
    const b = open(root);
    await a.put({ key: "k/a", value: 1, identity });
    const foreign = await connect(root);
    await foreign.put({ key: "k/b", value: 2, identity });
    const exports = vi.spyOn(SqliteStateStore.prototype, "exportLive");
    const deltas = vi.spyOn(SqliteStateStore.prototype, "readDelta");
    const first = a.stateToken();
    expect(b.stateToken()).toBe(first);
    expect(b.listAllShared("").map((entry) => entry.value)).toEqual([1, 2]);
    expect(exports.mock.calls.length + deltas.mock.calls.length).toBe(1);
    // A foreign commit: the first reader brings it forward, the second adopts it.
    await foreign.put({ key: "k/c", value: 3, identity });
    const second = b.stateToken();
    expect(second).not.toBe(first);
    expect(a.stateToken()).toBe(second);
    expect(exports.mock.calls.length + deltas.mock.calls.length).toBe(2);
    // The older token still pins the older state in both stores.
    expect(a.get("k/c", { snapshot: first })).toBeUndefined();
    expect(b.get("k/c", { snapshot: second })?.value).toBe(3);
    expectEqualsExport(a, foreign.exportState({ live: true }));
  });
});

describe("sqlite afterCommit and commitOutbox", () => {
  it("run exactly once per committed batch and never for a rolled-back one", async () => {
    const store = open(tempRoot("after"));
    const raw = sqliteOf(store).store;
    let after = 0;
    let outbox = 0;
    const hooks = { afterCommit: () => { after += 1; }, commitOutbox: () => { outbox += 1; } };
    await store.writeBatch({ identity, ops: [{ kind: "put", key: "k/a", value: 1 }], ...hooks });
    expect([after, outbox]).toEqual([1, 1]);
    // A conflict aborts and rolls back: neither runs.
    await expect(store.writeBatch({ identity, ops: [{ kind: "put", key: "k/b", value: 1 },
      { kind: "put", key: "k/a", value: 2, ifVersion: 99 }], ...hooks })).rejects.toBeInstanceOf(MeshBatchConflictError);
    // A throwing prepare rolls back too.
    await expect(store.writeBatch({ identity, ops: [{ kind: "put", key: "k/b", value: 1 }], prepare: () => { throw new Error("no"); },
      ...hooks })).rejects.toThrow("no");
    expect([after, outbox]).toEqual([1, 1]);
    expect(store.get("k/b")).toBeUndefined();
    // A write-free batch commits nothing and runs each once.
    await store.writeBatch({ identity, ops: [], prepare: () => [], ...hooks });
    expect([after, outbox]).toEqual([2, 2]);
    // afterCommit of a changing batch re-acquires custody; the commit hook never does.
    const reacquired = raw.stats().afterCommitReacquired;
    expect(reacquired).toBe(1);
    await store.writeBatch({ identity, ops: [{ kind: "put", key: "k/c", value: 1 }], commitOutbox: hooks.commitOutbox });
    expect(outbox).toBe(3);
    expect(raw.stats().afterCommitReacquired).toBe(reacquired);
  });
});
