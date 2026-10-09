import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { isMeshLockTimeout } from "../src/core/atomic-write.js";
import { normalizeFabricConfig } from "../src/config.js";
import { MESH_STATE_BUSY_CODE, MeshShadowNotReconciledError, MeshStateBusyError, MeshStateFileReadChangedError, resolveMeshStateBackend,
  ShadowStateBackend, SqliteStateBackend, type MeshCommitEffects } from "../src/mesh/state-backend.js";
import { MeshStateRetiredError, openNodeSqlite, SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { MeshBatchConflictError, MeshStore, type MeshBatchOperation, type MeshIdentity, type MeshStateEntry,
  type MeshStoreOptions } from "../src/mesh/store.js";

const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const other: MeshIdentity = { id: "other", name: "other", kind: "actor" };
const roots: string[] = [];
const stores: MeshStore[] = [];

const tempRoot = (label: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-state-backend-${label}-`));
  roots.push(root);
  return root;
};

const open = (root: string, options: MeshStoreOptions = {}): MeshStore => {
  const store = new MeshStore(root, 64 * 1024, 1_000, options);
  stores.push(store);
  return store;
};

const shadowOf = (store: MeshStore): ShadowStateBackend => {
  const backend = store.stateBackendHandle;
  if (!(backend instanceof ShadowStateBackend)) throw new Error(`not a shadow backend: ${backend.kind}`);
  return backend;
};

beforeEach(() => { vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", ""); });

afterEach(() => {
  vi.unstubAllEnvs();
  for (const store of stores.splice(0)) try { store.closeState(); } catch { /* closed by the test */ }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// Reads without the wall clock: updatedAt differs between two runs by construction.
const strip = (entry: MeshStateEntry | undefined): unknown => entry && { ...entry, updatedAt: 0 };
const outcome = async (run: () => Promise<unknown>): Promise<unknown> => {
  try {
    const value = await run();
    return { ok: Array.isArray(value) ? value : value && typeof value === "object" && "key" in value ? strip(value as MeshStateEntry) : value };
  } catch (error) { return { error: (error as Error).constructor.name, message: (error as Error).message }; }
};
const prng = (seed: number) => () => {
  seed = (seed * 1_103_515_245 + 12_345) & 0x7fffffff;
  return seed / 0x7fffffff;
};

describe("state backend selection", () => {
  it("defaults to file; an explicit option wins; the environment overrides only when nothing is explicit", () => {
    expect(resolveMeshStateBackend(undefined, undefined)).toBe("file");
    expect(resolveMeshStateBackend(undefined, "sqlite")).toBe("sqlite");
    expect(resolveMeshStateBackend(undefined, " Shadow ")).toBe("shadow");
    expect(resolveMeshStateBackend(undefined, "bogus")).toBe("file");
    expect(resolveMeshStateBackend("file", "sqlite")).toBe("file");
    expect(() => resolveMeshStateBackend("bogus" as "file")).toThrow(/mesh.stateBackend/);
    const root = tempRoot("select");
    expect(open(path.join(root, "a")).stateBackend).toBe("file");
    vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", "sqlite");
    expect(open(path.join(root, "b")).stateBackend).toBe("sqlite");
    expect(open(path.join(root, "c"), { stateBackend: "file" }).stateBackend).toBe("file");
  });

  it("parses mesh.stateBackend with the environment override", () => {
    expect(normalizeFabricConfig({}).mesh.stateBackend).toBe("file");
    expect(normalizeFabricConfig({ mesh: { stateBackend: "shadow" } }).mesh.stateBackend).toBe("shadow");
    expect(() => normalizeFabricConfig({ mesh: { stateBackend: "postgres" } })).toThrow(/mesh.stateBackend/);
    vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", "sqlite");
    expect(normalizeFabricConfig({ mesh: { stateBackend: "file" } }).mesh.stateBackend).toBe("sqlite");
    vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", "nope");
    expect(normalizeFabricConfig({ mesh: { stateBackend: "shadow" } }).mesh.stateBackend).toBe("shadow");
  });

  it("the file backend creates no SQLite file", async () => {
    const root = tempRoot("file-only");
    const store = open(root);
    await store.put({ key: "a", value: 1, identity });
    expect(fs.existsSync(path.join(root, "state.json"))).toBe(true);
    expect(fs.readdirSync(root).filter(name => name.startsWith("state.db") || name === "state-shadow")).toEqual([]);
    expect(store.stateDiagnostics()).toEqual({ kind: "file" });
  });
});

describe("file and sqlite give identical reads for the same operation sequence", () => {
  const script = async (store: MeshStore, random: () => number): Promise<unknown[]> => {
    const trace: unknown[] = [];
    const keys = ["a", "b", "c", "ns/x", "ns/y", "ns:z"];
    const pick = (): string => keys[Math.floor(random() * keys.length)]!;
    trace.push(await outcome(() => store.put({ key: "a", value: { n: 1 }, identity })));
    trace.push(await outcome(() => store.put({ key: "a", value: { n: 2 }, identity, ifVersion: 0 })));
    trace.push(await outcome(() => store.put({ key: "b", value: [1, 2], identity: other, ifVersion: 0 })));
    trace.push(await outcome(() => store.delete({ key: "b", ifVersion: 99 })));
    trace.push(await outcome(() => store.delete({ key: "b" })));
    trace.push(await outcome(() => store.delete({ key: "b" })));
    trace.push(await outcome(() => store.writeBatch({ identity, ops: [
      { kind: "put", key: "c", value: "c1" },
      { kind: "put", key: "a", value: "stale", ifVersion: 1, onConflict: "skip" },
      { kind: "put", key: "b", value: "again", ifVersion: 0, onConflict: "skip" },
    ] })));
    trace.push(await outcome(() => store.writeBatch({ identity, ops: [
      { kind: "put", key: "c", value: "lost" },
      { kind: "put", key: "a", value: "stale", ifVersion: 1, onConflict: "abort" },
    ] })));
    trace.push(await outcome(() => store.writeBatch({ identity, ops: [], prepare: view => {
      trace.push({ view: view.listAll("").map(strip), versions: keys.map(key => view.version(key)) });
      return [{ kind: "delete", key: "c", condition: get => get("a") !== undefined }];
    } })));
    for (let step = 0; step < 150; step += 1) {
      const key = pick();
      const roll = random();
      const version = store.get(key)?.version ?? 0;
      const guess = roll < 0.5 ? version : Math.floor(random() * 4);
      if (roll < 0.35) trace.push(await outcome(() => store.put({ key, value: { step, roll }, identity, ifVersion: guess })));
      else if (roll < 0.5) trace.push(await outcome(() => store.delete({ key, ...(random() < 0.5 ? { ifVersion: guess } : {}) })));
      else if (roll < 0.8) {
        const ops: MeshBatchOperation[] = [
          { kind: "put", key, value: step, identity: other, ifVersion: guess, onConflict: random() < 0.5 ? "skip" : "abort" },
          { kind: "delete", key: pick(), onConflict: "skip" },
        ];
        trace.push(await outcome(() => store.writeBatch({ identity, ops })));
      } else trace.push(await outcome(() => store.put({ key, value: `v${step}`, identity })));
      trace.push(keys.map(read => strip(store.get(read))));
    }
    trace.push(store.listAll("").map(strip), store.list("ns", 1).map(strip), store.listAll("ns/").map(strip));
    return trace;
  };

  it("in a scripted and a seeded random sequence", async () => {
    const file = open(tempRoot("diff-file"), { stateBackend: "file" });
    const sqlite = open(tempRoot("diff-sqlite"), { stateBackend: "sqlite" });
    expect(sqlite.stateBackend).toBe("sqlite");
    const fileTrace = await script(file, prng(7));
    const sqliteTrace = await script(sqlite, prng(7));
    expect(sqliteTrace).toEqual(fileTrace);
    expect(fileTrace.some(step => JSON.stringify(step).includes("MeshBatchConflictError"))).toBe(true);
  });

  it("snapshot tokens pin reads in sqlite", async () => {
    const store = open(tempRoot("token"), { stateBackend: "sqlite" });
    await store.put({ key: "a", value: 1, identity });
    const token = store.stateToken();
    expect(store.stateToken()).toBe(token);
    await store.put({ key: "a", value: 2, identity });
    expect(store.get("a", { snapshot: token })?.value).toBe(1);
    expect(store.get("a")?.value).toBe(2);
    expect(store.stateToken()).not.toBe(token);
    expect(store.listAllShared("").map(entry => entry.value)).toEqual([2]);
  });
});

describe.each(["file", "sqlite"] as const)("R11 callbacks on the %s backend", (kind) => {
  it("re-reads when the fileRead stamp changes before the transaction and gives prepare the fresh read", async () => {
    const store = open(tempRoot(`fileread-${kind}`), { stateBackend: kind });
    let stamps = 0;
    let reads = 0;
    const seen: unknown[] = [];
    const results = await store.writeBatch({
      identity, ops: [],
      fileRead: {
        read: () => { reads += 1; return `read-${reads}`; },
        // The first read's bracket (stamps 1 and 2) holds; the file changes before the transaction
        // re-checks it (stamp 3), so the read is redone once.
        stamp: () => ((stamps += 1) <= 2 ? "before" : "after"),
      },
      prepare: (_view, value) => { seen.push(value); return [{ kind: "put", key: "k", value }]; },
    });
    expect(reads).toBe(2);
    expect(seen).toEqual(["read-2"]);
    expect(results).toEqual([{ key: "k", applied: true, version: 1 }]);
    expect(store.get("k")?.value).toBe("read-2");
  });

  it("a write between read() and the stamp forces a retry and never commits the stale read", async () => {
    const root = tempRoot(`fileread-race-${kind}`);
    const source = path.join(root, "source.txt");
    fs.writeFileSync(source, "v1");
    let reads = 0;
    const seen: unknown[] = [];
    const store = open(root, { stateBackend: kind });
    const results = await store.writeBatch({
      identity, ops: [],
      fileRead: {
        read: () => {
          reads += 1;
          const value = fs.readFileSync(source, "utf8");
          // A concurrent writer lands after the read returned, before any later stamp.
          if (reads === 1) fs.writeFileSync(source, "v2");
          return value;
        },
        stamp: () => fs.readFileSync(source, "utf8"),
      },
      prepare: (_view, value) => { seen.push(value); return [{ kind: "put", key: "k", value }]; },
    });
    expect(reads).toBe(2);
    expect(seen).toEqual(["v2"]);
    expect(results).toEqual([{ key: "k", applied: true, version: 1 }]);
    expect(store.get("k")?.value).toBe("v2");
  });

  it("gives up with MeshStateFileReadChangedError and writes nothing when the stamp never holds", async () => {
    const store = open(tempRoot(`fileread-fail-${kind}`), { stateBackend: kind });
    let tick = 0;
    await expect(store.writeBatch({
      identity, ops: [{ kind: "put", key: "k", value: 1 }],
      fileRead: { read: () => undefined, stamp: () => String(tick++), retries: 2 },
    })).rejects.toBeInstanceOf(MeshStateFileReadChangedError);
    expect(store.get("k")).toBeUndefined();
  });

  it("runs commitOutbox once after COMMIT, outside the state lock, with the committed effects", async () => {
    const root = tempRoot(`outbox-${kind}`);
    const store = open(root, { stateBackend: kind });
    await store.put({ key: "keep", value: 0, identity });
    const effects: MeshCommitEffects[] = [];
    const lockHeld: boolean[] = [];
    const results = await store.writeBatch({
      identity, ops: [{ kind: "put", key: "k", value: "v" }, { kind: "put", key: "keep", value: 9, ifVersion: 5, onConflict: "skip" }],
      commitOutbox: (effect) => { effects.push(effect); lockHeld.push(fs.existsSync(path.join(root, ".lock"))); },
    });
    expect(effects).toHaveLength(1);
    expect(lockHeld).toEqual([false]);
    const [effect] = effects;
    expect(effect!.backend).toBe(kind);
    expect(effect!.results).toEqual(results);
    expect(effect!.changed).toEqual(["k"]);
    expect(effect!.stamp).toBe(store.stateStamp());
    expect(effect!.view.get("k")?.value).toBe("v");
    expect(effect!.view.version("k")).toBe(2);
    // A write-free batch still reaches the outbox, with nothing changed.
    await store.writeBatch({ identity, ops: [], commitOutbox: (effect) => { effects.push(effect); } });
    expect(effects[1]!.changed).toEqual([]);
  });

  it("a throwing commitOutbox rejects the call but the commit stands; a conflict never reaches it", async () => {
    const store = open(tempRoot(`outbox-throw-${kind}`), { stateBackend: kind });
    await expect(store.writeBatch({ identity, ops: [{ kind: "put", key: "k", value: 1 }],
      commitOutbox: () => { throw new Error("outbox down"); } })).rejects.toThrow("outbox down");
    expect(store.get("k")?.value).toBe(1);
    let called = false;
    await expect(store.writeBatch({ identity, ops: [{ kind: "put", key: "k", value: 2, ifVersion: 7 }],
      commitOutbox: () => { called = true; } })).rejects.toBeInstanceOf(MeshBatchConflictError);
    expect(called).toBe(false);
  });
});

describe("sqlite backend acquisition", () => {
  it("never takes the mesh .lock for state", async () => {
    const root = tempRoot("nolock");
    const store = open(root, { stateBackend: "sqlite" });
    // A fixture's first open of a fresh root installs the marker under the import's fence (custody.lock and
    // .lock, smarty-dev#6477 review round 2): that is initialisation, not state, so it happens before watching.
    expect(store.listAll("")).toEqual([]);
    const seen: string[] = [];
    const watcher = fs.watch(root, (_event, name) => { if (name) seen.push(String(name)); });
    try {
      await store.put({ key: "a", value: 1, identity });
      await store.delete({ key: "a" });
      await store.writeBatch({ identity, ops: [{ kind: "put", key: "b", value: 2 }], afterCommit: () => undefined });
      await store.confirmWritable();
      await new Promise(resolve => setTimeout(resolve, 50));
    } finally { watcher.close(); }
    expect(seen.filter(name => name.startsWith(".lock"))).toEqual([]);
    // state.json is only the moved marker a fresh root's initialisation leaves (smarty-dev#6477), never state.
    expect(JSON.parse(fs.readFileSync(path.join(root, "state.json"), "utf8"))).toMatchObject({ format: "sqlite", movedTo: "state.db", epoch: 1 });
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(true);
  });

  it("honours the busy budget asynchronously and maps SQLITE_BUSY to a counted MeshLockTimeoutError", async () => {
    const root = tempRoot("busy");
    const store = open(root, { stateBackend: "sqlite", lockTimeoutMs: 200 });
    await store.put({ key: "a", value: 1, identity });
    const holder = openNodeSqlite(path.join(root, "state.db"));
    holder.exec("BEGIN IMMEDIATE");
    let ticks = 0;
    const ticker = setInterval(() => { ticks += 1; }, 5);
    try {
      const started = performance.now();
      const error = await store.put({ key: "a", value: 2, identity }).then(() => undefined, (caught: unknown) => caught);
      const waited = performance.now() - started;
      expect(error).toBeInstanceOf(MeshStateBusyError);
      expect(isMeshLockTimeout(error)).toBe(true);
      expect((error as MeshStateBusyError).busyCode).toBe(MESH_STATE_BUSY_CODE);
      expect((error as Error).message).toContain(MESH_STATE_BUSY_CODE);
      expect(waited).toBeGreaterThanOrEqual(190);
      expect(waited).toBeLessThan(2_000);
      // The event loop kept running during the wait: the wait is asynchronous (a synchronous wait
      // gives 0 ticks). Windows timers resolve at ~15.6 ms and CI hosts are loaded, so the 5 ms
      // interval fires far fewer than waited / 5 times; require a few ticks, not a rate.
      expect(ticks).toBeGreaterThanOrEqual(3);
      // A withTryLock budget bounds it further, as for .lock.
      const tryStarted = performance.now();
      await expect(store.withTryLock(() => store.put({ key: "a", value: 3, identity }), 20)).rejects.toBeInstanceOf(MeshStateBusyError);
      expect(performance.now() - tryStarted).toBeLessThan(150);
      expect(store.stateDiagnostics().busyTimeouts).toBe(2);
    } finally {
      clearInterval(ticker);
      holder.exec("ROLLBACK");
      holder.close();
    }
    expect((await store.put({ key: "a", value: 4, identity })).value).toBe(4);
  });

  it("stops waiting when the writeSignal aborts", async () => {
    const root = tempRoot("abort");
    const controller = new AbortController();
    const store = open(root, { stateBackend: "sqlite", writeSignal: controller.signal });
    await store.put({ key: "a", value: 1, identity });
    const holder = openNodeSqlite(path.join(root, "state.db"));
    holder.exec("BEGIN IMMEDIATE");
    try {
      const pending = store.put({ key: "a", value: 2, identity });
      setTimeout(() => controller.abort(new Error("lifetime over")), 30);
      await expect(pending).rejects.toThrow("lifetime over");
    } finally {
      holder.exec("ROLLBACK");
      holder.close();
    }
  });
});

describe("sqlite readers notice a retired state.db (pi-fabric#626 review round 3)", () => {
  const sqliteOf = (store: MeshStore): SqliteStateBackend => {
    const backend = store.stateBackendHandle;
    if (!(backend instanceof SqliteStateBackend)) throw new Error(`not a sqlite backend: ${backend.kind}`);
    return backend;
  };

  it("a cached snapshot is not reused after another SqliteStateStore retires the database", async () => {
    const root = tempRoot("retired-cache");
    const a = open(root, { stateBackend: "sqlite" });
    await a.put({ key: "r/1", value: 1, identity });
    const token = a.stateToken();                                        // A caches a snapshot
    expect(a.list("r/").map(entry => entry.value)).toEqual([1]);
    expect(a.get("r/1", { snapshot: token })?.value).toBe(1);
    const before = a.stateStamp();
    const b = await SqliteStateStore.open(root, 64 * 1024, 1_000);   // a separate connection
    try { expect(await b.retire()).toBe(2); } finally { b.close(); }
    // Retirement leaves commit_no unchanged, but the stamp reads the current epoch and backend.
    expect(a.stateStamp()).toBeDefined();
    expect(a.stateStamp()).not.toBe(before);
    expect(() => a.list("r/")).toThrow(MeshStateRetiredError);
    expect(() => a.listAllShared("")).toThrow(MeshStateRetiredError);
    expect(() => a.stateToken()).toThrow(MeshStateRetiredError);
    expect(() => a.get("r/1", { snapshot: token })).toThrow(MeshStateRetiredError);
    expect(() => a.get("r/1")).toThrow(MeshStateRetiredError);
    expect(() => a.listAll("")).toThrow(MeshStateRetiredError);
  });

  it("a cache miss never exports a retired database (another connection, retirement flag only)", async () => {
    const root = tempRoot("retired-miss");
    const a = open(root, { stateBackend: "sqlite" });
    await a.put({ key: "r/1", value: 1, identity });
    const before = a.stateStamp();
    // Another connection (as another process would) flips only the flag: no epoch bump, no commit_no.
    const raw = openNodeSqlite(path.join(root, "state.db"));
    try { raw.exec("UPDATE meta SET value = 'retired' WHERE name = 'backend'"); } finally { raw.close(); }
    expect(a.stateStamp()).not.toBe(before);
    expect(() => a.list("r/")).toThrow(MeshStateRetiredError);      // first snapshot: a miss
    expect(() => a.stateToken()).toThrow(MeshStateRetiredError);
    const store = sqliteOf(a).store;
    expect(() => store.exportState({ live: true })).toThrow(MeshStateRetiredError);
    expect(() => store.changesSince(0)).toThrow(MeshStateRetiredError);
    // Maintenance (rollback) still exports the retired database explicitly.
    expect(store.exportState()).toMatchObject({ backend: "retired", entries: { "r/1": { value: 1 } } });
  });

  it("a store's own stamp changes when another store retires and stays stable otherwise", async () => {
    const root = tempRoot("retired-stamp");
    const a = await SqliteStateStore.open(root, 64 * 1024, 1_000);
    const b = await SqliteStateStore.open(root, 64 * 1024, 1_000);
    try {
      await a.put({ key: "s/1", value: 1, identity });
      const live = a.stateStamp();
      expect(a.stateStamp()).toBe(live);
      expect(a.exportState({ live: true }).entries["s/1"]?.value).toBe(1);
      await b.retire();
      expect(a.stateStamp()).not.toBe(live);
      expect(() => a.assertLive()).toThrow(MeshStateRetiredError);
      expect(() => a.exportState({ live: true })).toThrow(MeshStateRetiredError);
      // b retired it itself: its stamp moved too, and its own live reads fail closed.
      expect(() => b.exportState({ live: true })).toThrow(MeshStateRetiredError);
    } finally { a.close(); b.close(); }
  });
});

describe("shadow backend", () => {
  it("keeps the file authoritative, mirrors committed values, and detects and repairs a divergence", async () => {
    const root = tempRoot("shadow");
    const store = open(root, { stateBackend: "shadow" });
    expect(store.stateBackend).toBe("shadow");
    const shadow = shadowOf(store);
    await store.put({ key: "a", value: { n: 1 }, identity });
    await store.writeBatch({ identity, ops: [{ kind: "put", key: "b", value: 2 }, { kind: "put", key: "c", value: 3 }] });
    await store.delete({ key: "c" });
    await shadow.flush();
    expect(fs.existsSync(path.join(root, "state.json"))).toBe(true);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
    expect(shadow.shadow.listAll("").map(entry => [entry.key, entry.value, entry.updatedBy])).toEqual([
      ["a", { n: 1 }, identity], ["b", 2, identity],
    ]);
    expect(await shadow.verify()).toEqual([]);
    // A writer that does not mirror (an older binary, a file-backend process) changes the file only.
    const foreign = open(root, { stateBackend: "file" });
    await foreign.put({ key: "a", value: "foreign", identity: other });
    const first = await shadow.verify();
    expect(first.map(difference => difference.key)).toEqual(["a"]);
    expect(first[0]!.file).toEqual({ value: "foreign", updatedBy: other });
    expect(store.stateDiagnostics().divergences).toBe(0);  // could still be another process's pending mirror
    await shadow.verify();
    expect(store.stateDiagnostics()).toMatchObject({ kind: "shadow", divergences: 1, divergentKeys: ["a"], shadowFailures: 0 });
    await shadow.repair();
    expect(await shadow.verify()).toEqual([]);
    expect(shadow.shadow.get("a")?.value).toBe("foreign");
  });

  it("mirrors a committed batch even when afterCommit or commitOutbox throws", async () => {
    const store = open(tempRoot("shadow-callback-throw"), { stateBackend: "shadow" });
    const shadow = shadowOf(store);
    await expect(store.writeBatch({ identity, ops: [{ kind: "put", key: "a", value: 1 }],
      afterCommit: () => { throw new Error("after down"); } })).rejects.toThrow("after down");
    await expect(store.writeBatch({ identity, ops: [{ kind: "put", key: "b", value: 2 }],
      commitOutbox: () => { throw new Error("outbox down"); } })).rejects.toThrow("outbox down");
    expect(store.get("a")?.value).toBe(1);
    expect(store.get("b")?.value).toBe(2);
    await shadow.flush();
    expect(shadow.shadow.listAll("").map(entry => [entry.key, entry.value])).toEqual([["a", 1], ["b", 2]]);
    expect(await shadow.verify()).toEqual([]);
    // A rejected batch (nothing committed) mirrors nothing.
    await expect(store.writeBatch({ identity, ops: [{ kind: "put", key: "c", value: 3, ifVersion: 9 }] }))
      .rejects.toBeInstanceOf(MeshBatchConflictError);
    await shadow.flush();
    expect(shadow.shadow.get("c")).toBeUndefined();
  });

  it("seeds the shadow from the existing file state on first use", async () => {
    const root = tempRoot("shadow-seed");
    const before = open(root, { stateBackend: "file" });
    await before.put({ key: "old", value: 1, identity });
    const store = open(root, { stateBackend: "shadow" });
    await store.put({ key: "new", value: 2, identity });
    await shadowOf(store).flush();
    expect(shadowOf(store).shadow.listAll("").map(entry => entry.key)).toEqual(["new", "old"]);
    expect(await shadowOf(store).verify()).toEqual([]);
  });

  it("verify() before any write runs the initial reconcile first (smarty-dev#6900)", async () => {
    const root = tempRoot("shadow-verify-first");
    const before = open(root, { stateBackend: "file" });
    await before.put({ key: "a", value: 1, identity });
    await before.put({ key: "b", value: { nested: true }, identity });
    const store = open(root, { stateBackend: "shadow" });
    const shadow = shadowOf(store);
    // No write, no repair, no flush: verify() is the first use.
    expect(await shadow.verify()).toEqual([]);
    expect(shadow.shadow.listAll("").map(entry => entry.key)).toEqual(["a", "b"]);
    // Repeated (timer) checks stay clean and never count a divergence.
    expect(await shadow.verify()).toEqual([]);
    expect(store.stateDiagnostics()).toMatchObject({ kind: "shadow", divergences: 0, divergentKeys: [], shadowFailures: 0 });
  });

  it("verify() never compares before the first reconcile: a failed one is retried, else 'not reconciled' (pi-fabric#671)", async () => {
    const root = tempRoot("shadow-verify-unreconciled");
    const before = open(root, { stateBackend: "file" });
    await before.put({ key: "a", value: 1, identity });
    await before.put({ key: "b", value: 2, identity });
    const store = open(root, { stateBackend: "shadow" });
    const shadow = shadowOf(store);
    const busy = (): Error => Object.assign(new Error("SQLITE_BUSY: database is locked"), { errcode: 5 });
    // The initial full mirror (started by the first write) fails, e.g. SQLite busy.
    const failing = vi.spyOn(shadow.shadow, "writeBatch").mockRejectedValue(busy());
    await store.put({ key: "c", value: 3, identity });
    await shadow.flush();
    expect(shadow.shadow.listAll("")).toEqual([]);
    // While it keeps failing, verify() retries (bounded) and then says so: never divergences.
    await expect(shadow.verify()).rejects.toBeInstanceOf(MeshShadowNotReconciledError);
    await expect(shadow.verify()).rejects.toThrow(/not reconciled/);
    expect(store.stateDiagnostics()).toMatchObject({ divergences: 0, divergentKeys: [] });
    expect(store.stateDiagnostics().shadowFailures).toBe(1 + 2 * 3);
    // One more transient failure: verify() retries the reconcile itself and compares a clean shadow.
    failing.mockReset();
    failing.mockRejectedValueOnce(busy());
    failing.mockImplementation(SqliteStateBackend.prototype.writeBatch.bind(shadow.shadow) as never);
    expect(await shadow.verify()).toEqual([]);
    expect(shadow.shadow.listAll("").map(entry => entry.key)).toEqual(["a", "b", "c"]);
    expect(await shadow.verify()).toEqual([]);
    expect(store.stateDiagnostics()).toMatchObject({ divergences: 0, divergentKeys: [], shadowFailures: 8 });
    failing.mockRestore();
  });

  it("a SQLite failure never fails or changes the caller's write", async () => {
    const root = tempRoot("shadow-fail");
    fs.writeFileSync(path.join(root, "state-shadow"), "not a directory");
    const store = open(root, { stateBackend: "shadow" });
    expect((await store.put({ key: "a", value: 1, identity })).version).toBe(1);
    expect(await store.writeBatch({ identity, ops: [{ kind: "put", key: "b", value: 2 }] }))
      .toEqual([{ key: "b", applied: true, version: 2 }]);
    await shadowOf(store).flush();
    expect(store.get("a")?.value).toBe(1);
    expect(store.stateDiagnostics().shadowFailures).toBeGreaterThan(0);
  });

  it("a busy shadow database does not delay the caller beyond its own budget", async () => {
    const root = tempRoot("shadow-busy");
    const store = open(root, { stateBackend: "shadow" });
    await store.put({ key: "a", value: 1, identity });
    await shadowOf(store).flush();
    const holder = openNodeSqlite(path.join(root, "state-shadow", "state.db"));
    holder.exec("BEGIN IMMEDIATE");
    try {
      const started = performance.now();
      await store.put({ key: "a", value: 2, identity });
      expect(performance.now() - started).toBeLessThan(200);
      await shadowOf(store).flush();
      expect(store.stateDiagnostics().shadowFailures).toBe(1);
    } finally {
      holder.exec("ROLLBACK");
      holder.close();
    }
    expect(store.get("a")?.value).toBe(2);
    await store.put({ key: "b", value: 3, identity });
    await shadowOf(store).flush();
    await shadowOf(store).repair();
    expect(await shadowOf(store).verify()).toEqual([]);
  });
});
