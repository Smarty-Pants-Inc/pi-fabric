import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { isMeshLockTimeout } from "../src/core/atomic-write.js";
import { isMeshRetryableBusy, isMeshStateBusy } from "../src/mesh/state-backend.js";
import { StoreBridgeSide } from "../src/mesh/bridge.js";
import { checkpointFlagRaised, lowerCheckpointFlag, ownerToken, raiseCheckpointFlag, reclaimClaimPath, removeOwnedFile, SqliteStateStore, tryLockFile }
  from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshIdentity, type MeshStoreOptions } from "../src/mesh/store.js";

// smarty-dev#6477 P0 (sqlite soak): wal_autocheckpoint=0 and no production checkpointer let the hub's WAL grow
// to 48 MB, and a raw "database is locked" (SQLITE_BUSY) escaped as fatal and stopped a mesh-bridge on restart.

type Database = { exec(sql: string): void; close(): void; prepare(sql: string): { get(): unknown } };
const { DatabaseSync } = process.getBuiltinModule("node:sqlite") as { DatabaseSync: new (file: string, options?: object) => Database };

const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
const stores: MeshStore[] = [];
const sqlite: SqliteStateStore[] = [];
const databases: Database[] = [];

const tempRoot = (label: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-sqmaint-${label}-`));
  roots.push(root);
  return root;
};
const open = (root: string, options: MeshStoreOptions = {}): MeshStore => {
  const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "sqlite", ...options });
  stores.push(store);
  return store;
};
const openStore = async (root: string, options: Parameters<typeof SqliteStateStore.open>[3] = {}): Promise<SqliteStateStore> => {
  const store = await SqliteStateStore.open(root, 256 * 1024, 1_000, { initialize: "create", ...options });
  sqlite.push(store);
  return store;
};
const raw = (root: string): Database => {
  const database = new DatabaseSync(path.join(root, "state.db"), { timeout: 0 });
  databases.push(database);
  return database;
};
/** Another process's exclusive hold: every other connection's read, write and checkpoint is SQLITE_BUSY. */
const holdExclusive = (root: string): Database => {
  const database = raw(root);
  database.exec("PRAGMA locking_mode = EXCLUSIVE");
  database.exec("BEGIN EXCLUSIVE");
  database.exec("UPDATE meta SET value = value WHERE name = 'created_at'");
  return database;
};
const release = (database: Database): void => {
  try { database.exec("ROLLBACK"); } catch { /* none open */ }
  database.close();
  databases.splice(databases.indexOf(database), 1);
};
const failure = (operation: () => unknown): unknown => {
  try { operation(); } catch (error) { return error; }
  throw new Error("expected a failure");
};
const expectRetryableBusy = (error: unknown): void => {
  expect(error).toBeInstanceOf(Error);
  expect((error as Error).message).not.toBe("database is locked");
  expect(isMeshStateBusy(error)).toBe(true);
  expect(isMeshLockTimeout(error)).toBe(true);
};
const walBytes = (root: string): number => fs.statSync(path.join(root, "state.db-wal"), { throwIfNoEntry: false })?.size ?? 0;
const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
const pad = "x".repeat(6_000);

afterEach(() => {
  vi.restoreAllMocks();
  for (const database of databases.splice(0)) try { database.close(); } catch { /* closed */ }
  for (const store of stores.splice(0)) try { store.closeState(); } catch { /* closed */ }
  for (const store of sqlite.splice(0)) try { store.close(); } catch { /* closed */ }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("SQLite busy never escapes raw (defect 2)", () => {
  it("maps a busy first open, busy reads and busy writes to the retryable FABRIC_MESH_STATE_BUSY", async () => {
    const root = tempRoot("busy");
    const seed = open(root);
    await seed.put({ key: "a", value: 1, identity });
    seed.closeState();

    let holder = holdExclusive(root);
    const fresh = open(root, { lockTimeoutMs: 150 });
    expectRetryableBusy(failure(() => fresh.get("a"))); // the synchronous first open
    expectRetryableBusy(failure(() => fresh.listAll("")));
    const write = await fresh.put({ key: "b", value: 2, identity }).then(() => undefined, (error: unknown) => error);
    expectRetryableBusy(write); // the asynchronous first open of a write
    release(holder);
    expect(fresh.get("a")?.value).toBe(1);

    holder = raw(root); // now on an OPEN store: another connection holds the write lock (EXCLUSIVE needs it closed)
    holder.exec("BEGIN IMMEDIATE");
    holder.exec("UPDATE meta SET value = value WHERE name = 'created_at'");
    expect(fresh.get("a")?.value).toBe(1); // WAL readers never wait for the writer
    expectRetryableBusy(await fresh.put({ key: "b", value: 2, identity }).then(() => undefined, (error: unknown) => error));
    release(holder);
    expect((await fresh.put({ key: "b", value: 2, identity })).version).toBeGreaterThan(0);
  });

  it("opening an imported root needs no write lock (a held writer does not fail a restart)", async () => {
    const root = tempRoot("open");
    const seed = open(root);
    await seed.put({ key: "a", value: 1, identity });
    seed.closeState();
    const writer = raw(root);
    writer.exec("BEGIN IMMEDIATE"); // another process holds the state write lock, as on a busy hub
    const restarted = open(root);
    expect(restarted.get("a")?.value).toBe(1);
    writer.exec("ROLLBACK");
  });

  it("a raw driver busy error is retryable for the bridge loop", async () => {
    const root = tempRoot("raw");
    const seed = open(root);
    await seed.put({ key: "a", value: 1, identity });
    seed.closeState();
    const holder = holdExclusive(root);
    const reader = raw(root);
    const error = failure(() => reader.prepare("SELECT count(*) FROM kv").get());
    expect((error as Error).message).toMatch(/locked|busy/i);
    expect(isMeshRetryableBusy(error)).toBe(true);
    expect(isMeshRetryableBusy(new Error("database disk image is malformed"))).toBe(false);
    release(holder);
  });

  it("a bridge side on a freshly opened store under an exclusive hold fails retryably, then recovers", async () => {
    const root = tempRoot("bridge");
    const seed = open(root);
    await seed.put({ key: "a", value: 1, identity });
    seed.closeState();
    const holder = holdExclusive(root);
    const side = new StoreBridgeSide(open(root, { lockTimeoutMs: 150 }), "remote-machine");
    for (const operation of [() => side.presence(), () => side.mirror({ hosts: [], participants: [] })]) {
      const error = await operation().then(() => undefined, (failure: unknown) => failure);
      if (error !== undefined) {
        expect((error as Error).message).not.toMatch(/^database is locked$/);
        expect(isMeshRetryableBusy(error)).toBe(true);
      }
    }
    release(holder);
    await expect(side.presence()).resolves.toBeDefined();
  });
});

describe("WAL stays bounded without a maintainer (defect 1)", () => {
  it("SQLite's built-in autocheckpoint is on and keeps a client-only WAL near 4 MiB", async () => {
    const root = tempRoot("wal");
    const store = await openStore(root);
    let max = 0;
    for (let n = 0; n < 1_500; n += 1) { // ~9 MB of commits, no maintainer, below the 64 MiB emergency threshold
      await store.put({ key: `k${n % 50}`, value: { n, pad }, identity });
      max = Math.max(max, walBytes(root));
    }
    expect(max).toBeLessThan(6 * 1024 * 1024);
    expect(store.stats().checkpoints.emergency).toBe(0);
  });
});

describe("synchronous busy retries stay inside their wall-clock budget (PR #691 review P2)", () => {
  const sleepSync = (ms: number): void => { Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms); };
  const timed = (operation: () => unknown): { error: unknown; ms: number } => {
    const started = performance.now();
    const error = failure(operation);
    return { error, ms: performance.now() - started };
  };

  it("a persistently busy read on an open store blocks for <= ~15 ms, busy-handler waits included", async () => {
    const root = tempRoot("p2-read");
    const store = open(root, { busyTimeoutMs: 5 } as MeshStoreOptions); // the maximum handler wait per attempt
    await store.put({ key: "a", value: 1, identity });
    expect(store.get("a")?.value).toBe(1); // opened
    let attempts = 0;
    vi.spyOn(SqliteStateStore.prototype, "listAll").mockImplementation(() => {
      attempts += 1;
      sleepSync(5); // what SQLite's busy handler spends before it gives up
      throw Object.assign(new Error("database is locked"), { errcode: 5 });
    });
    const { error, ms } = timed(() => store.listAll(""));
    expectRetryableBusy(error);
    expect(attempts).toBeGreaterThan(1); // it does retry
    // Six attempts at 5 ms plus 15 ms of sleeps blocked ~45 ms before; the budget is 15 ms (+ timer slack).
    expect(ms).toBeLessThan(15 + (process.platform === "win32" ? 25 : 10)); // Windows timer ~15.6 ms (smarty-dev#7630)
  });

  it("a busy first open under an exclusive hold blocks for <= ~40 ms, busy-handler waits included", async () => {
    const root = tempRoot("p2-open");
    const seed = open(root);
    await seed.put({ key: "a", value: 1, identity });
    seed.closeState();
    const holder = holdExclusive(root);
    const fresh = open(root, { busyTimeoutMs: 5, lockTimeoutMs: 150 } as MeshStoreOptions);
    const { error, ms } = timed(() => fresh.get("a"));
    expectRetryableBusy(error);
    expect(ms).toBeLessThan(40 + 15);
    release(holder);
    expect(fresh.get("a")?.value).toBe(1);
  });
});

describe("WAL stays bounded with constant concurrent readers (full-load soak defect)", () => {
  // Readers that always overlap: a new read transaction starts every 2 ms and each one ends ~6 ms later, so
  // some reader always holds a WAL read mark and a PASSIVE checkpoint can never let the WAL restart.
  const overlappingReaders = (root: string): () => void => {
    const connections = [raw(root), raw(root), raw(root), raw(root)];
    let next = 0;
    const timer = setInterval(() => {
      const reader = connections[next % connections.length]!;
      const old = connections[(next + connections.length - 3) % connections.length]!;
      try { old.exec("COMMIT"); } catch { /* none open */ }
      try { reader.exec("COMMIT"); } catch { /* none open */ }
      reader.exec("BEGIN");
      reader.prepare("SELECT count(*) AS n FROM kv").get();
      next += 1;
    }, 2);
    return () => {
      clearInterval(timer);
      for (const connection of connections) try { connection.exec("COMMIT"); } catch { /* none open */ }
    };
  };
  const run = async (walResetBytes: number): Promise<{ max: number; elapsedMs: number; stats: ReturnType<SqliteStateStore["stats"]> }> => {
    const root = tempRoot(`readers-${walResetBytes}`);
    const store = await openStore(root, { walResetBytes });
    const stop = overlappingReaders(root);
    const started = Date.now();
    let max = 0;
    try {
      for (let n = 0; n < 1_400; n += 1) { // ~16 MiB of commits, below the 64 MiB emergency threshold
        await store.put({ key: `k${n % 50}`, value: { n, pad }, identity });
        max = Math.max(max, walBytes(root));
        if (n % 25 === 0) await sleep(1); // let the readers' timer and an in-flight reset run
      }
      await sleep(50);
    } finally { stop(); }
    return { max, elapsedMs: Date.now() - started, stats: store.stats() };
  };

  // pi-fabric#694 P1-B: the reset makes ONE non-blocking attempt and never waits for readers to drain, so
  // against readers that ALWAYS overlap it defers (a later commit tries again after the 2 s back-off) and the
  // WAL stays bounded by the emergency path; with the readers gone a later commit resets it (the P1-B test).
  it("against always-overlapping readers the reset makes one non-blocking attempt per back-off and defers", async () => {
    const control = await run(0); // the reset off: starvation reproduces
    console.info(`WAL with readers: control max ${control.max}`);
    expect(control.max).toBeGreaterThan(12 * 1024 * 1024);
    const fixed = await run(1024 * 1024);
    const { walResets, walResetBusy, emergency } = fixed.stats.checkpoints;
    console.info(`WAL with readers: reset max ${fixed.max}, resets ${walResets}, busy ${walResetBusy}, ${fixed.elapsedMs} ms`);
    expect(walResets + walResetBusy).toBeGreaterThan(0);
    // At most one attempt per back-off window (plus the first): no retry loop.
    expect(walResets + walResetBusy).toBeLessThanOrEqual(Math.floor(fixed.elapsedMs / 2_000) + 1 + walResets);
    expect(emergency).toBe(0);
  }, 60_000);
});

describe("a busy WAL reset defers to a later commit (pi-fabric#694 P1-B)", () => {
  it("a busy reader makes the reset defer without sleeping, and a commit after the back-off truncates", async () => {
    const root = tempRoot("reset-defer");
    const store = await openStore(root, { walResetBytes: 1 });
    const realNow = Date.now.bind(Date);
    let skew = 0;
    const clock = vi.spyOn(Date, "now").mockImplementation(() => realNow() + skew);
    const turn = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));
    const commit = async (n: number): Promise<void> => {
      skew += 300; // past the 250 ms check throttle
      await store.put({ key: `k${n}`, value: { n, pad }, identity });
      await turn();
      await turn();
    };
    try {
      await commit(0);
      const before = store.stats().checkpoints;
      const reader = raw(root);
      reader.exec("BEGIN");
      reader.prepare("SELECT count(*) AS n FROM kv").get(); // pins the WAL: TRUNCATE is busy
      await commit(1);
      // Settled within two event-loop turns: one attempt, no sleep or retry, and no lock or flag left behind.
      let stats = store.stats().checkpoints;
      expect(stats.walResetBusy).toBe(before.walResetBusy + 1);
      expect(stats.walResets).toBe(before.walResets);
      expect(fs.existsSync(path.join(root, "state-wal-reset.lock"))).toBe(false);
      expect(checkpointFlagRaised(root)).toBe(false);
      await commit(2); // inside the 2 s back-off: no attempt
      stats = store.stats().checkpoints;
      expect(stats.walResetBusy).toBe(before.walResetBusy + 1);
      expect(walBytes(root)).toBeGreaterThan(0);
      reader.exec("COMMIT");
      skew += 2_000;
      await commit(3); // after the back-off: this commit drives the reset
      stats = store.stats().checkpoints;
      expect(stats.walResets).toBe(before.walResets + 1);
      expect(stats.walResetBusy).toBe(before.walResetBusy + 1);
      expect(walBytes(root)).toBe(0);
    } finally {
      clock.mockRestore();
    }
  });
});

// pi-fabric#694 P2-E: an existing state.db (and its -wal/-shm) is opened only when it is ours.
describe.skipIf(process.platform === "win32")("an existing state.db must be private (pi-fabric#694 P2-E)", () => {
  const refused = async (root: string, sync = false): Promise<unknown> => {
    try {
      if (sync) sqlite.push(SqliteStateStore.openSync(root, 256 * 1024, 1_000, { initialize: "create" }));
      else await openStore(root);
    } catch (error) { return error; }
    throw new Error("expected a refusal");
  };
  const prepared = async (label: string): Promise<string> => {
    const root = tempRoot(label);
    (await openStore(root)).close();
    return root;
  };

  it("refuses a group- or other-writable state.db, sync and async", async () => {
    const root = await prepared("db-mode");
    fs.chmodSync(path.join(root, "state.db"), 0o666);
    expect(await refused(root)).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED", message: expect.stringMatching(/state\.db: it is group or other writable/) });
    expect(await refused(root, true)).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED" });
  });

  it("tightens an owned 0644/0640 state.db, -wal and -shm to 0600 before opening (astra c6076385095)", async () => {
    const root = await prepared("db-readable");
    const db = path.join(root, "state.db");
    const held = await openStore(root); // keeps -wal and -shm present
    for (const [name, mode] of [[db, 0o644], [db + "-wal", 0o640], [db + "-shm", 0o644]] as const) fs.chmodSync(name, mode);
    const second = await openStore(root);
    for (const name of [db, db + "-wal", db + "-shm"]) expect(fs.statSync(name).mode & 0o777).toBe(0o600);
    second.close(); held.close();
    fs.chmodSync(db, 0o640);
    sqlite.push(SqliteStateStore.openSync(root, 256 * 1024, 1_000, { initialize: "create" }));
    expect(fs.statSync(db).mode & 0o777).toBe(0o600);
  });

  it("creates a new state.db and its -wal/-shm owner-only", async () => {
    const root = tempRoot("db-new-mode");
    const store = await openStore(root);
    await store.put({ key: "k", value: { v: 1 }, identity: { id: "t", name: "t" } } as never).catch(() => undefined);
    for (const name of ["state.db", "state.db-wal", "state.db-shm"]) {
      const st = fs.statSync(path.join(root, name), { throwIfNoEntry: false });
      if (st) expect(st.mode & 0o077).toBe(0);
    }
    store.close();
  });

  it("refuses a root directory that others can write", async () => {
    const root = await prepared("root-mode");
    fs.chmodSync(root, 0o777);
    try { expect(await refused(root)).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED", message: expect.stringMatching(/group or other writable/) }); }
    finally { fs.chmodSync(root, 0o700); }
  });

  it("refuses a symlinked state.db and a symlinked -wal, never following them", async () => {
    const root = await prepared("db-link");
    const target = path.join(tempRoot("db-target"), "state.db");
    fs.renameSync(path.join(root, "state.db"), target);
    fs.symlinkSync(target, path.join(root, "state.db"));
    expect(await refused(root)).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED", message: expect.stringMatching(/symbolic link/) });
    const other = await prepared("wal-link");
    const victim = path.join(tempRoot("wal-target"), "victim");
    fs.writeFileSync(victim, "untouched");
    fs.rmSync(path.join(other, "state.db-wal"), { force: true });
    fs.symlinkSync(victim, path.join(other, "state.db-wal"));
    expect(await refused(other)).toMatchObject({ message: expect.stringMatching(/state\.db-wal: it is a symbolic link/) });
    expect(fs.readFileSync(victim, "utf8")).toBe("untouched");
  });

  it("still opens a private existing state.db", async () => {
    const root = await prepared("db-ok");
    expect(await openStore(root)).toBeInstanceOf(SqliteStateStore);
  });
});

describe("the WAL-reset try-lock (pi-fabric#691 review P2)", () => {
  const lockRoot = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wal-lock-")); roots.push(root); return root; };
  const age = (file: string, ms: number) => { const at = new Date(Date.now() - ms); fs.utimesSync(file, at, at); };

  it("reclaims an ownerless stale lock and refuses a fresh one", () => {
    const lock = path.join(lockRoot(), "state-wal-reset.lock");
    fs.writeFileSync(lock, "crashed\n");
    expect(tryLockFile(lock, 2_000, ownerToken())).toBe(false);
    age(lock, 5_000);
    expect(tryLockFile(lock, 2_000, ownerToken())).toBe(true);
    expect(fs.readdirSync(path.dirname(lock))).toEqual(["state-wal-reset.lock"]);
  });

  // pi-fabric#694 P2-C: liveness, not age, decides a lock with a readable owner.
  it("never reclaims a live owner's lock on age alone", () => {
    const lock = path.join(lockRoot(), "state-wal-reset.lock");
    const live = ownerToken();
    expect(tryLockFile(lock, 2_000, live)).toBe(true);
    age(lock, 60_000);
    expect(tryLockFile(lock, 2_000, ownerToken())).toBe(false);
    expect(fs.readFileSync(lock, "utf8")).toBe(`${live}\n`);
  });

  it("reclaims a fresh lock whose owner is dead on this host (pid gone, or pid reused)", () => {
    const lock = path.join(lockRoot(), "state-wal-reset.lock");
    const gone = spawnSync(process.execPath, ["-e", ""]).pid; // exited and reaped
    fs.writeFileSync(lock, `${gone}.12345.00000000-0000-0000-0000-000000000000\n`);
    const mine = ownerToken();
    expect(tryLockFile(lock, 2_000, mine)).toBe(true);
    expect(fs.readFileSync(lock, "utf8")).toBe(`${mine}\n`);
    if (process.platform !== "linux") return; // start ticks come from /proc
    fs.rmSync(lock);
    fs.writeFileSync(lock, `${process.pid}.1.00000000-0000-0000-0000-000000000000\n`); // this pid, another start
    expect(tryLockFile(lock, 2_000, mine)).toBe(true);
    expect(fs.readdirSync(path.dirname(lock))).toEqual(["state-wal-reset.lock"]);
  });

  // pi-fabric#694 P2 (smarty-dev#6477): a reclaim never moves the shared name aside. A reclaimer paused after its
  // verdict, while another reclaims and holds, and a third tries, ends with exactly one holder and its record at
  // the canonical name: no `.stale` file, no leftover claim.
  const dead = (): string => `${spawnSync(process.execPath, ["-e", ""]).pid}.12345.00000000-0000-0000-0000-000000000000\n`;
  const entries = (lock: string) => fs.readdirSync(path.dirname(lock)).sort();

  it("A reclaim vs B reclaim vs C acquire of one dead lock: B alone holds, A never touches it", () => {
    const lock = path.join(lockRoot(), "state-wal-reset.lock");
    fs.writeFileSync(lock, dead());
    const a = ownerToken();
    const b = ownerToken();
    const c = ownerToken();
    let bHolds = false;
    let cHolds = false;
    const aHolds = tryLockFile(lock, 2_000, a, { afterVerdict: () => {
      bHolds = tryLockFile(lock, 2_000, b); // B reclaims the same dead record and holds it now
      cHolds = tryLockFile(lock, 2_000, c); // C tries before A goes on
    } });
    cHolds ||= tryLockFile(lock, 2_000, c);
    expect([aHolds, bHolds, cHolds]).toEqual([false, true, false]);
    expect(fs.readFileSync(lock, "utf8")).toBe(`${b}\n`);
    expect(entries(lock)).toEqual(["state-wal-reset.lock"]);
  });

  it("the reviewer's interleaving: B replaces the lock after A's check, C tries before A goes on: B alone holds", () => {
    const lock = path.join(lockRoot(), "state-wal-reset.lock");
    fs.writeFileSync(lock, "crashed\n");
    age(lock, 5_000);
    const b = ownerToken();
    const c = ownerToken();
    let cHolds = false;
    const aHolds = tryLockFile(lock, 2_000, ownerToken(), { afterClaim: () => {
      fs.rmSync(lock); // replaced behind A's back (no code path does this to a dead record A has claimed)
      fs.writeFileSync(lock, `${b}\n`);
      cHolds = tryLockFile(lock, 2_000, c);
    } });
    cHolds ||= tryLockFile(lock, 2_000, c);
    expect([aHolds, cHolds]).toEqual([false, false]);
    expect(fs.readFileSync(lock, "utf8")).toBe(`${b}\n`);
    expect(entries(lock)).toEqual(["state-wal-reset.lock"]);
  });

  it("while A holds the claim on a dead record, no other reclaimer or creator can vacate or take the name", () => {
    const lock = path.join(lockRoot(), "state-wal-reset.lock");
    fs.writeFileSync(lock, dead());
    const a = ownerToken();
    const others: boolean[] = [];
    const aHolds = tryLockFile(lock, 2_000, a, { afterClaim: () => {
      others.push(tryLockFile(lock, 2_000, ownerToken()), tryLockFile(lock, 2_000, ownerToken()));
    } });
    expect([aHolds, ...others]).toEqual([true, false, false]);
    expect(fs.readFileSync(lock, "utf8")).toBe(`${a}\n`);
    expect(entries(lock)).toEqual(["state-wal-reset.lock"]);
  });

  it("a claim left by a dead reclaimer is reclaimed one level down, never blocking recovery for good", () => {
    const lock = path.join(lockRoot(), "state-wal-reset.lock");
    const record = dead();
    fs.writeFileSync(lock, record);
    fs.writeFileSync(reclaimClaimPath(lock, record), dead()); // a reclaimer that died holding the claim
    const mine = ownerToken();
    expect(tryLockFile(lock, 2_000, mine)).toBe(true);
    expect(fs.readFileSync(lock, "utf8")).toBe(`${mine}\n`);
    expect(entries(lock)).toEqual(["state-wal-reset.lock"]);
  });

  it("never deletes a fresh lock that another reclaimer created after this process saw the stale one", () => {
    const lock = path.join(lockRoot(), "state-wal-reset.lock");
    fs.writeFileSync(lock, "crashed\n");
    age(lock, 5_000);
    const lstatSync = fs.lstatSync.bind(fs);
    let raced = false;
    const spy = vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: fs.StatSyncOptions) => {
      const result = lstatSync(file, options as fs.StatSyncOptions & { throwIfNoEntry: false });
      if (!raced && String(file) === lock) {
        raced = true; // another process reclaims the stale lock and holds a fresh one
        fs.rmSync(lock);
        fs.writeFileSync(lock, "other holder\n");
      }
      return result;
    }) as typeof fs.lstatSync);
    try {
      expect(tryLockFile(lock, 2_000, ownerToken())).toBe(false);
    } finally {
      spy.mockRestore();
    }
    expect(raced).toBe(true);
    expect(fs.readFileSync(lock, "utf8")).toBe("other holder\n");
    expect(fs.readdirSync(path.dirname(lock))).toEqual(["state-wal-reset.lock"]);
  });

  it("concurrent checkpoint requests each own a flag: one finishing never lowers another's", () => {
    const root = lockRoot();
    expect(checkpointFlagRaised(root)).toBe(false);
    const first = raiseCheckpointFlag(root, ownerToken());
    const second = raiseCheckpointFlag(root, ownerToken());
    expect(checkpointFlagRaised(root)).toBe(true);
    lowerCheckpointFlag(second); // the later request finishes first
    expect(checkpointFlagRaised(root)).toBe(true); // the earlier one is still active: writers keep yielding
    lowerCheckpointFlag(first);
    expect(checkpointFlagRaised(root)).toBe(false);
    // A crashed request's flag goes stale and stops blocking writers.
    const crashed = raiseCheckpointFlag(root, ownerToken());
    age(crashed, 5_000);
    expect(checkpointFlagRaised(root)).toBe(false);
  });

  // pi-fabric#694 P2-D: an existing flag directory is never followed or written through unless it is ours.
  it.skipIf(process.platform === "win32")("refuses a symlinked or group-writable flag directory and never writes through it", () => {
    const root = lockRoot();
    const elsewhere = lockRoot();
    fs.symlinkSync(elsewhere, path.join(root, "state-checkpoint.flags"));
    expect(() => raiseCheckpointFlag(root, ownerToken())).toThrow(/state-checkpoint\.flags: it is a symbolic link/);
    expect(fs.readdirSync(elsewhere)).toEqual([]);
    fs.writeFileSync(path.join(elsewhere, "fresh"), ""); // a foreign "flag" never makes writers yield
    expect(checkpointFlagRaised(root)).toBe(false);
    const shared = lockRoot();
    fs.mkdirSync(path.join(shared, "state-checkpoint.flags"), { mode: 0o700 });
    fs.chmodSync(path.join(shared, "state-checkpoint.flags"), 0o777);
    expect(() => raiseCheckpointFlag(shared, ownerToken())).toThrow(expect.objectContaining({ code: "FABRIC_MESH_STATE_UNSUPPORTED" }));
    expect(fs.readdirSync(path.join(shared, "state-checkpoint.flags"))).toEqual([]);
  });

  it("a release removes only the releaser's own lock, never one another process holds", () => {
    const dir = lockRoot();
    const lock = path.join(dir, "state-wal-reset.lock");
    const mine = ownerToken();
    expect(tryLockFile(lock, 2_000, mine)).toBe(true);
    // Another process reclaimed it (a stall past the stale age) and holds it now.
    const theirs = ownerToken();
    fs.rmSync(lock);
    fs.writeFileSync(lock, `${theirs}\n`);
    removeOwnedFile(lock, mine);
    expect(fs.readFileSync(lock, "utf8")).toBe(`${theirs}\n`);
    removeOwnedFile(lock, theirs);
    expect(fs.existsSync(lock)).toBe(false);
  });
});
