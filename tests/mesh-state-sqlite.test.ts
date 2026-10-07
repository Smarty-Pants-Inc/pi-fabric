import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import {
  filesystemRefusal, MeshStateRetiredError, openNodeSqlite, SqliteStateStore, type SqliteConnection, type SqliteStateStoreOptions,
} from "../src/mesh/state-sqlite.js";
import { MeshBatchConflictError, MeshStore, type MeshBatchOperation, type MeshIdentity } from "../src/mesh/store.js";

const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
const stores: SqliteStateStore[] = [];

const tempRoot = (label: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-state-sqlite-${label}-`));
  roots.push(root);
  return root;
};

const open = async (root: string, options: SqliteStateStoreOptions = {}): Promise<SqliteStateStore> => {
  const store = await SqliteStateStore.open(root, 64 * 1024, 1_000, options);
  stores.push(store);
  return store;
};

afterEach(() => {
  for (const store of stores.splice(0)) try { store.close(); } catch { /* closed by the test */ }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// Raw node:sqlite on the same file, from another connection: an independent observer/holder.
const raw = (root: string) => openNodeSqlite(path.join(root, "state.db"));

const CHILD_PRELUDE = `
import { createJiti } from "jiti";
import { pathToFileURL } from "node:url";
import fs from "node:fs";
import path from "node:path";
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const { SqliteStateStore } = await jiti.import("./src/mesh/state-sqlite.ts");
const identity = { id: "child-" + process.pid, name: "child", kind: "agent" };
const spin = (until) => { while (!until()) { /* hold the transaction synchronously */ } };
`;

interface ChildResult { code: number | null; signal: NodeJS.Signals | null; stdout: string; stderr: string }

const child = (code: string, args: string[]): { pid: number; kill: (signal: NodeJS.Signals) => void; done: Promise<ChildResult> } => {
  const processHandle = spawn(process.execPath, ["--input-type=module", "-e", CHILD_PRELUDE + code, ...args],
    { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] });
  let stdout = "";
  let stderr = "";
  processHandle.stdout.on("data", (chunk) => { stdout += chunk; });
  processHandle.stderr.on("data", (chunk) => { stderr += chunk; });
  const done = new Promise<ChildResult>((resolve, reject) => {
    processHandle.once("error", reject);
    processHandle.once("close", (exit, signal) => resolve({ code: exit, signal, stdout, stderr }));
  });
  done.catch(() => undefined);
  return { pid: processHandle.pid!, kill: (signal) => { processHandle.kill(signal); }, done };
};

const waitFor = async (predicate: () => boolean, timeoutMs = 30_000, describe = "condition"): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${describe}`);
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};

// A deterministic PRNG for the differential run.
const prng = (seed: number) => () => {
  seed = (seed * 1_103_515_245 + 12_345) & 0x7fffffff;
  return seed / 0x7fffffff;
};

const outcome = async <T>(run: () => Promise<T>): Promise<unknown> => {
  try { return { ok: await run() }; }
  catch (error) { return { error: (error as Error).constructor.name, message: (error as Error).message }; }
};

describe("SqliteStateStore", () => {
  it("opens WAL with synchronous=NORMAL, no client autocheckpoint, a clamped busy timeout and a 0600 file", async () => {
    const root = tempRoot("pragmas");
    const executed: string[] = [];
    const recording = (file: string): SqliteConnection => {
      const inner = openNodeSqlite(file);
      return { exec: (sql) => { executed.push(sql); inner.exec(sql); }, prepare: (sql) => inner.prepare(sql),
        close: () => inner.close(), get isTransaction() { return inner.isTransaction; } };
    };
    const store = await open(root, { open: recording, busyTimeoutMs: 10_000 });
    await store.put({ key: "a/1", value: 1, identity });
    expect(executed).toEqual(expect.arrayContaining([
      "PRAGMA busy_timeout = 5", "PRAGMA synchronous = NORMAL", "PRAGMA wal_autocheckpoint = 0", "BEGIN IMMEDIATE", "COMMIT",
    ]));
    const observer = raw(root);
    expect(observer.prepare("PRAGMA journal_mode").get()).toEqual({ journal_mode: "wal" });
    observer.close();
    if (process.platform !== "win32") expect(fs.statSync(store.file).mode & 0o777).toBe(0o600);
  });

  it("matches the file store's revision, CAS, tombstone and batch contract op for op", async () => {
    const file = new MeshStore(tempRoot("diff-file"), 64 * 1024, 1_000, { maxStateTombstones: 3 });
    const sqlite = await open(tempRoot("diff-sqlite"), { maxStateTombstones: 3 });
    const random = prng(6477);
    const keys = ["a/1", "a/2", "a/3", "b/1", "b/2", "b/3", "c/1", "c/2"];
    const pick = (): string => keys[Math.floor(random() * keys.length)]!;
    const versions = (store: MeshStore | SqliteStateStore) => {
      let seen: Record<string, number> = {};
      return store.writeBatch({ identity, ops: [], prepare: (view) => {
        seen = Object.fromEntries(keys.map(key => [key, view.version(key)]));
        return [];
      } }).then(() => seen);
    };
    const strip = (entries: Array<{ key: string; value: unknown; version: number; updatedBy: MeshIdentity }>) =>
      entries.map(({ key, value, version, updatedBy }) => ({ key, value, version, updatedBy }));
    for (let step = 0; step < 400; step += 1) {
      const roll = random();
      const key = pick();
      const stale = random() < 0.25;
      const fileVersion = (await versions(file))[key]!;
      const ifVersion = random() < 0.5 ? undefined : stale ? fileVersion + 1 : fileVersion;
      let left: unknown;
      let right: unknown;
      if (roll < 0.4) {
        const value = { step, text: "v".repeat(step % 7) };
        left = await outcome(() => file.put({ key, value, identity, ...(ifVersion === undefined ? {} : { ifVersion }) }).then(e => e.version));
        right = await outcome(() => sqlite.put({ key, value, identity, ...(ifVersion === undefined ? {} : { ifVersion }) }).then(e => e.version));
      } else if (roll < 0.7) {
        left = await outcome(() => file.delete({ key, ...(ifVersion === undefined ? {} : { ifVersion }) }));
        right = await outcome(() => sqlite.delete({ key, ...(ifVersion === undefined ? {} : { ifVersion }) }));
      } else {
        const ops: MeshBatchOperation[] = [];
        const count = 1 + Math.floor(random() * 3);
        for (let index = 0; index < count; index += 1) {
          const target = pick();
          const cas = random() < 0.5 ? { ifVersion: Math.floor(random() * 6), onConflict: random() < 0.5 ? "skip" as const : "abort" as const } : {};
          ops.push(random() < 0.6
            ? { kind: "put", key: target, value: (now: number) => ({ step, index, at: typeof now }), ...cas }
            : { kind: "delete", key: target, ...cas });
        }
        left = await outcome(() => file.writeBatch({ identity, ops }));
        right = await outcome(() => sqlite.writeBatch({ identity, ops }));
      }
      expect(right, `step ${step}`).toEqual(left);
      if (step % 25 === 0 || step === 399) {
        expect(strip(sqlite.listAll("")), `entries at step ${step}`).toEqual(strip(file.listAll("")));
        expect(strip(sqlite.listAll("a/"))).toEqual(strip(file.listAll("a/")));
        expect(await versions(sqlite), `versions at step ${step}`).toEqual(await versions(file));
      }
    }
    const exported = sqlite.exportState();
    expect(exported.tombstoneOrder.length).toBeLessThanOrEqual(3);
    const fileState = JSON.parse(fs.readFileSync(path.join(file.root, "state.json"), "utf8")) as { highWater: number; tombstoneOrder: string[] };
    expect(exported.highWater).toBe(fileState.highWater);
    expect(exported.tombstoneOrder).toEqual(fileState.tombstoneOrder);
  }, 60_000);

  it("runs prepare and the operations on one snapshot and afterCommit only after a commit", async () => {
    const store = await open(tempRoot("batch"), { maxStateBytes: 4_096 }); // maxEventBytes 64 KiB: the floor is 128 KiB
    await store.put({ key: "hosts/a", value: { owner: "a" }, identity });
    const seen: unknown[] = [];
    const results = await store.writeBatch({
      identity,
      ops: [{ kind: "put", key: "hosts/b", value: (now: number) => ({ owner: "b", at: now }) }],
      prepare: (view) => {
        seen.push(view.listAll("hosts/").map(entry => entry.key));
        return [{ kind: "put", key: "hosts/a", value: { owner: "a2" }, ifVersion: view.version("hosts/a") },
          { kind: "delete", key: "hosts/a", condition: () => false }];
      },
      afterCommit: (view) => { seen.push(view.get("hosts/a")?.value, typeof (view.get("hosts/b")?.value as { at?: unknown }).at); },
    });
    expect(results).toEqual([
      // An absent key takes the clock's successor; a present key its own successor (as the file store).
      { key: "hosts/b", applied: true, version: 2 }, { key: "hosts/a", applied: true, version: 2 },
      { key: "hosts/a", applied: false, version: 2 },
    ]);
    expect(seen).toEqual([["hosts/a"], { owner: "a2" }, "number"]);
    expect(store.stats().afterCommitReacquired).toBe(1);

    // An abort conflict rolls back the operations already applied in the batch.
    await expect(store.writeBatch({ identity, ops: [
      { kind: "put", key: "hosts/c", value: 1 },
      { kind: "put", key: "hosts/a", value: 2, ifVersion: 1 },
    ], afterCommit: () => { seen.push("never"); } })).rejects.toBeInstanceOf(MeshBatchConflictError);
    expect(store.get("hosts/c")).toBeUndefined();

    // A no-op batch runs afterCommit on its exact snapshot without a second acquisition.
    await store.writeBatch({ identity, ops: [], afterCommit: (view) => { seen.push(view.version("hosts/b")); } });
    expect(seen.at(-1)).toBe(2);
    expect(store.stats().afterCommitReacquired).toBe(1);

    // A commit that fails (here: the aggregate state cap) never runs afterCommit.
    const small = await open(tempRoot("cap"), { maxStateBytes: 1 }); // floor: 2 x maxEventBytes = 128 KiB
    await expect(small.writeBatch({ identity, ops: [
      { kind: "put", key: "big/1", value: "x".repeat(60_000) }, { kind: "put", key: "big/2", value: "x".repeat(60_000) },
      { kind: "put", key: "big/3", value: "x".repeat(60_000) },
    ], afterCommit: () => { seen.push("never"); } })).rejects.toThrow(/exceeds 131072 bytes/);
    expect(small.listAll("")).toEqual([]);
    expect(small.exportState().highWater).toBe(0);
    expect(seen).not.toContain("never");

    // Callbacks are synchronous and cannot re-enter a writer.
    await expect(store.writeBatch({ identity, ops: [],
      prepare: (() => Promise.resolve([])) as unknown as () => MeshBatchOperation[] })).rejects.toThrow(/must be synchronous/);
    let inner: Promise<unknown> | undefined;
    await store.writeBatch({ identity, ops: [], prepare: () => {
      inner = store.put({ key: "hosts/z", value: 1, identity });
      inner.catch(() => undefined);
      return [];
    } });
    await expect(inner).rejects.toThrow(/must not call store writers/);
    expect(store.get("hosts/z")).toBeUndefined();
  });

  it("evicts the oldest tombstones without ever reissuing a revision", async () => {
    const store = await open(tempRoot("tombstones"), { maxStateTombstones: 2 });
    for (const key of ["t/1", "t/2", "t/3", "t/4"]) await store.put({ key, value: key, identity });
    for (const key of ["t/1", "t/2", "t/3", "t/4"]) await store.delete({ key });
    const exported = store.exportState();
    expect(exported.tombstoneOrder).toEqual(["t/3", "t/4"]);
    expect(exported.highWater).toBe(8);
    await expect(store.put({ key: "t/3", value: 1, identity, ifVersion: 0 })).rejects.toThrow(/compare-and-swap/);
    const recreated = await store.put({ key: "t/1", value: "again", identity, ifVersion: 0 });
    expect(recreated.version).toBe(9);
    const changes = store.changesSince(0);
    expect(changes.complete).toBe(true);
    expect(changes.changes.at(-1)).toEqual({ commit: changes.commit, key: "t/1", version: 9, deleted: false });
  });

  it("trims the change feed by whole commits: a range never reads half of one", async () => {
    const store = await open(tempRoot("trim"));
    // One commit of 4,097 changes crosses the 4,096-row retention boundary.
    await store.writeBatch({ identity, ops: Array.from({ length: 4_097 }, (_, index) => ({ kind: "put" as const, key: `big/${index}`, value: index })) });
    const big = store.changesSince(0);
    expect(big.commit).toBe(1);
    if (big.complete) expect(big.changes).toHaveLength(4_097);
    else expect(big.changes).toEqual([]);
    expect(big.complete).toBe(false); // the whole commit went: retention stays bounded
    await store.put({ key: "after/1", value: 1, identity });
    expect(store.changesSince(0).complete).toBe(false);
    const after = store.changesSince(1);
    expect(after.complete).toBe(true);
    expect(after.changes).toEqual([{ commit: 2, key: "after/1", version: 4_098, deleted: false }]);
    const observer = raw(store.root);
    try {
      expect({ ...observer.prepare("SELECT count(*) AS n, min(commit_no) AS oldest FROM changes").get() }).toEqual({ n: 1, oldest: 2 });
    } finally { observer.close(); }
  });

  it("imports a file-store snapshot and exports it back in the same envelope", async () => {
    const file = new MeshStore(tempRoot("import-file"), 64 * 1024, 1_000);
    for (let index = 0; index < 20; index += 1) await file.put({ key: `p/${index}`, value: { index }, identity });
    for (let index = 0; index < 5; index += 1) await file.delete({ key: `p/${index}` });
    const state = JSON.parse(fs.readFileSync(path.join(file.root, "state.json"), "utf8")) as Parameters<SqliteStateStore["importState"]>[0];
    const store = await open(tempRoot("import-sqlite"));
    expect(await store.importState(state)).toEqual({ entries: 15, tombstones: 5, highWater: 25 });
    const exported = store.exportState();
    expect(exported.entries).toEqual(state.entries);
    expect(exported.versions).toEqual(state.versions);
    expect(exported.tombstoneOrder).toEqual(state.tombstoneOrder);
    expect(store.listAll("p/")).toEqual(file.listAll("p/"));
    const tombstone = exported.versions["p/0"]!;
    expect((await store.put({ key: "p/0", value: 0, identity, ifVersion: tombstone })).version)
      .toBe((await file.put({ key: "p/0", value: 0, identity, ifVersion: tombstone })).version);
    await expect(store.importState(state)).rejects.toThrow(/empty database/);
  });

  it("imports a tombstone-only or metadata-only snapshot and finishes its metadata (review round 2 P1)", async () => {
    const store = await open(tempRoot("import-tombstones"));
    expect(await store.importState({ entries: {}, versions: { "g/1": 3, "g/2": 5 }, tombstoneOrder: ["g/1", "g/2"], highWater: 5 }))
      .toEqual({ entries: 0, tombstones: 2, highWater: 5 });
    expect(store.exportState()).toMatchObject({ highWater: 5, tombstoneOrder: ["g/1", "g/2"] });
    // New keys take the imported clock's successors (a present key its own successor, as the file
    // store), and two deletions take fresh tombstone ordinals instead of reusing imported ones.
    expect((await store.put({ key: "x/1", value: 1, identity })).version).toBe(6);
    expect((await store.put({ key: "x/2", value: 2, identity })).version).toBe(7);
    expect(await store.delete({ key: "x/1" })).toEqual({ deleted: true, version: 7 });
    expect(await store.delete({ key: "x/2" })).toEqual({ deleted: true, version: 8 });
    const after = store.exportState();
    expect(after.tombstoneOrder).toEqual(["g/1", "g/2", "x/1", "x/2"]);
    expect(after.highWater).toBeGreaterThanOrEqual(8);
    // Recreating an imported tombstone never reissues a revision.
    expect((await store.put({ key: "g/2", value: 1, identity, ifVersion: 5 })).version).toBeGreaterThan(after.highWater);

    // A metadata-only snapshot (no entries, no tombstones) still persists its clock and counts as an import.
    const empty = await open(tempRoot("import-meta"));
    expect(await empty.importState({ entries: {}, highWater: 9 })).toEqual({ entries: 0, tombstones: 0, highWater: 9 });
    expect(empty.exportState().highWater).toBe(9);
    expect((await empty.put({ key: "m/1", value: 1, identity })).version).toBe(10);
    await expect(empty.importState({ entries: {} })).rejects.toThrow(/empty database/);
  });

  it("fails closed on its own retirement after a read cached data_version (review round 2 P2)", async () => {
    const store = await open(tempRoot("retire-self"));
    await store.put({ key: "s/1", value: 1, identity });
    expect(store.get("s/1")?.value).toBe(1);
    expect(store.listAll("s/")).toHaveLength(1);
    expect(await store.retire()).toBe(2);
    expect(() => store.get("s/1")).toThrow(MeshStateRetiredError);
    expect(() => store.listAll("")).toThrow(MeshStateRetiredError);
    await expect(store.put({ key: "s/2", value: 2, identity })).rejects.toBeInstanceOf(MeshStateRetiredError);
  });

  it("retires in place: a writer that waited through the retirement and every reader fail closed", async () => {
    const root = tempRoot("retire");
    const store = await open(root);
    await store.put({ key: "r/1", value: 1, identity });
    const ready = path.join(root, "holder.ready");
    // Another process holds the write lock, retires the database inside it, then commits.
    const holder = child(`
      const db = new (process.getBuiltinModule("node:sqlite").DatabaseSync)(path.join(process.argv[1], "state.db"));
      db.exec("BEGIN IMMEDIATE");
      db.exec("UPDATE meta SET value = 'retired' WHERE name = 'backend'");
      db.exec("UPDATE meta SET value = 2 WHERE name = 'epoch'");
      fs.writeFileSync(process.argv[2], "");
      const until = Date.now() + 400; spin(() => Date.now() >= until);
      db.exec("COMMIT");`, [root, ready]);
    await waitFor(() => fs.existsSync(ready), 30_000, "holder");
    const started = Date.now();
    await expect(store.put({ key: "r/2", value: 2, identity })).rejects.toBeInstanceOf(MeshStateRetiredError);
    expect(Date.now() - started).toBeGreaterThan(100);
    expect((await holder.done).code).toBe(0);
    expect(() => store.get("r/1")).toThrow(MeshStateRetiredError);
    expect(store.exportState()).toMatchObject({ backend: "retired", epoch: 2, entries: { "r/1": { value: 1 } } });
    expect(Object.keys(store.exportState().entries)).toEqual(["r/1"]);
    await expect(SqliteStateStore.open(root, 64 * 1024, 1_000)).rejects.toBeInstanceOf(MeshStateRetiredError);

    // retire() itself: the other store's next write and read fail closed.
    const second = tempRoot("retire-api");
    const a = await open(second);
    const b = await open(second);
    await a.put({ key: "r/1", value: 1, identity });
    expect(await b.retire()).toBe(2);
    await expect(a.put({ key: "r/1", value: 2, identity })).rejects.toBeInstanceOf(MeshStateRetiredError);
    await expect(a.confirmWritable()).rejects.toBeInstanceOf(MeshStateRetiredError);
    expect(() => a.listAll("")).toThrow(MeshStateRetiredError);
  }, 60_000);

  it("waits asynchronously: timers keep running, try budgets and abort signals still hold", async () => {
    const root = tempRoot("async-wait");
    const store = await open(root);
    const controller = new AbortController();
    const abortable = await open(root, { writeSignal: controller.signal });
    const ready = path.join(root, "holder.ready");
    const holder = child(`
      const db = new (process.getBuiltinModule("node:sqlite").DatabaseSync)(path.join(process.argv[1], "state.db"));
      db.exec("BEGIN IMMEDIATE");
      fs.writeFileSync(process.argv[2], "");
      const until = Date.now() + 800; spin(() => Date.now() >= until);
      db.exec("COMMIT");`, [root, ready]);
    await waitFor(() => fs.existsSync(ready), 30_000, "holder");
    let lastTick = Date.now();
    let maxGap = 0;
    const timer = setInterval(() => { const now = Date.now(); maxGap = Math.max(maxGap, now - lastTick); lastTick = now; }, 5);
    try {
      const tried = Date.now();
      await expect(store.withTryLock(() => store.put({ key: "w/try", value: 1, identity }), 20)).rejects.toBeInstanceOf(MeshLockTimeoutError);
      expect(Date.now() - tried).toBeLessThan(400);
      const aborted = abortable.put({ key: "w/abort", value: 1, identity });
      setTimeout(() => controller.abort(new Error("quiesce")), 30);
      await expect(aborted).rejects.toThrow("quiesce");
      const waited = Date.now();
      lastTick = waited;
      maxGap = 0;
      expect((await store.put({ key: "w/1", value: 1, identity })).version).toBe(1);
      const done = Date.now();
      const elapsed = done - waited;
      maxGap = Math.max(maxGap, done - lastTick);
      expect(elapsed).toBeGreaterThan(100);
      // The event loop kept turning while the write waited (SQLite's own handler would block it for
      // the whole wait). A gap bound, not a tick count: Windows timers tick at ~15.6 ms, not 5 ms.
      expect(maxGap).toBeLessThan(elapsed / 2);
    } finally { clearInterval(timer); }
    expect((await holder.done).code).toBe(0);
    expect(store.stats().busyRetries).toBeGreaterThan(0);
    expect(store.stats().fifoJoins).toBeGreaterThan(0);
    expect(store.get("w/try")).toBeUndefined();
    expect(store.get("w/abort")).toBeUndefined();
  }, 60_000);

  it("kill -9 inside a write transaction rolls back and the kernel releases the lock", async () => {
    const root = tempRoot("crash");
    const store = await open(root);
    await store.put({ key: "c/0", value: "kept", identity });
    const ready = path.join(root, "crash.ready");
    const victim = child(`
      const store = await SqliteStateStore.open(process.argv[1], 65536, 1000);
      await store.writeBatch({ identity, ops: [
        { kind: "put", key: "c/1", value: "partial" },
        { kind: "delete", key: "c/0" },
        { kind: "put", key: "c/2", value: () => { fs.writeFileSync(process.argv[2], ""); spin(() => false); } },
      ] });`, [root, ready]);
    await waitFor(() => fs.existsSync(ready), 30_000, "victim inside its transaction");
    victim.kill("SIGKILL");
    expect((await victim.done).signal).toBe("SIGKILL");
    const started = Date.now();
    expect((await store.put({ key: "c/3", value: "after", identity })).version).toBe(2);
    expect(Date.now() - started).toBeLessThan(1_000);
    expect(store.get("c/1")).toBeUndefined();
    expect(store.get("c/0")?.value).toBe("kept");
    expect(store.exportState().highWater).toBe(2);
    const observer = raw(root);
    expect(observer.prepare("PRAGMA integrity_check").get()).toEqual({ integrity_check: "ok" });
    observer.close();
  }, 60_000);

  it("kill -9 between COMMIT and afterCommit keeps the commit and skips the effect", async () => {
    const root = tempRoot("crash-effect");
    const store = await open(root);
    const ready = path.join(root, "effect.ready");
    const effect = path.join(root, "effect.done");
    const victim = child(`
      const store = await SqliteStateStore.open(process.argv[1], 65536, 1000);
      await store.writeBatch({ identity, ops: [{ kind: "put", key: "e/1", value: "committed" }], afterCommit: (view) => {
        fs.writeFileSync(process.argv[2], JSON.stringify(view.get("e/1")));
        spin(() => false);
        fs.writeFileSync(process.argv[3], "");
      } });`, [root, ready, effect]);
    await waitFor(() => fs.existsSync(ready), 30_000, "victim inside afterCommit");
    // The effect saw the committed row, under custody.
    expect(JSON.parse(fs.readFileSync(ready, "utf8"))).toMatchObject({ key: "e/1", value: "committed", version: 1 });
    victim.kill("SIGKILL");
    await victim.done;
    expect(store.get("e/1")).toMatchObject({ value: "committed", version: 1 });
    expect(fs.existsSync(effect)).toBe(false);
    // The next round reconciles: its own afterCommit runs normally.
    let reconciled = false;
    await store.writeBatch({ identity, ops: [{ kind: "put", key: "e/1", value: "again", ifVersion: 1 }], afterCommit: () => { reconciled = true; } });
    expect(reconciled).toBe(true);
  }, 60_000);

  // 16 processes by default (host memory budget); PI_FABRIC_STATE_SQLITE_PROCS=40 reruns the design's 40.
  // The fill traffic is fixed (2,400 puts of 3 KB), so the WAL bound below discriminates at any count.
  const procs = Math.max(4, Number(process.env.PI_FABRIC_STATE_SQLITE_PROCS ?? 16));
  it(`${procs} processes: constant readers cannot starve the checkpoint, and CAS increments stay exact`, async () => {
    const root = tempRoot("many");
    const maintainer = await open(root, { checkpoint: "maintainer", checkpointIntervalMs: 50, checkpointBytes: 1024 * 1024 });
    await maintainer.put({ key: "ctr/shared", value: 0, identity });
    const go = path.join(root, "go");
    const stop = path.join(root, "stop");
    const writers = Math.ceil(procs / 2);
    const readers = procs - writers;
    const increments = 25;
    const fills = Math.ceil(2_400 / writers);
    const children = [
      ...Array.from({ length: writers }, (_, index) => child(`
        const [root, go, index, increments, fills] = process.argv.slice(1);
        const store = await SqliteStateStore.open(root, 65536, 1000);
        fs.writeFileSync(path.join(root, "ready-w" + index), "");
        while (!fs.existsSync(go)) await new Promise(r => setTimeout(r, 5));
        let conflicts = 0;
        for (let i = 0; i < Number(increments); i++) {
          for (;;) {
            const current = store.get("ctr/shared");
            try { await store.put({ key: "ctr/shared", value: current.value + 1, identity, ifVersion: current.version }); break; }
            catch (error) { if (!/compare-and-swap/.test(error.message)) throw error; conflicts++; }
          }
        }
        const filler = "f".repeat(3000);
        for (let i = 0; i < Number(fills); i++) await store.put({ key: "fill/" + index + "/" + (i % 8), value: { i, filler }, identity });
        console.log(JSON.stringify({ conflicts, stats: store.stats() }));
        store.close();`, [root, go, String(index), String(increments), String(fills)])),
      ...Array.from({ length: readers }, (_, index) => child(`
        const [root, go, stop, index] = process.argv.slice(1);
        const store = await SqliteStateStore.open(root, 65536, 1000);
        fs.writeFileSync(path.join(root, "ready-r" + index), "");
        while (!fs.existsSync(go)) await new Promise(r => setTimeout(r, 5));
        let reads = 0;
        // Back-to-back scans with no pause: overlapping read snapshots all the time.
        while (!fs.existsSync(stop)) { store.listAll("fill/"); store.get("ctr/shared"); reads++; }
        console.log(JSON.stringify({ reads }));
        store.close();`, [root, go, stop, String(index)])),
    ];
    await waitFor(() => fs.readdirSync(root).filter(name => name.startsWith("ready-")).length === writers + readers, 90_000, `${procs} children ready`);
    let maxWal = 0;
    const sampler = setInterval(() => { maxWal = Math.max(maxWal, maintainer.walBytes()); }, 10);
    fs.writeFileSync(go, "");
    const writerResults = await Promise.all(children.slice(0, writers).map(entry => entry.done));
    fs.writeFileSync(stop, "");
    const readerResults = await Promise.all(children.slice(writers).map(entry => entry.done));
    clearInterval(sampler);
    for (const result of [...writerResults, ...readerResults]) expect(result.code, result.stderr).toBe(0);
    expect(maintainer.get("ctr/shared")?.value).toBe(writers * increments);
    const reads = readerResults.reduce((sum, result) => sum + (JSON.parse(result.stdout) as { reads: number }).reads, 0);
    const stats = maintainer.stats();
    const walTraffic = writers * fills * 3_000;
    // Evidence for the PR: WAL bound versus the bytes written through it.
    console.log(JSON.stringify({ maxWal, walTraffic, reads, checkpoints: stats.checkpoints }));
    expect(reads).toBeGreaterThan(readers);
    expect(stats.checkpoints.truncate).toBeGreaterThan(0);
    // Without TRUNCATE under constant readers, the WAL only grows (review-opus P2-1 probe).
    expect(maxWal).toBeLessThan(Math.max(16 * 1024 * 1024, walTraffic / 2));
    maintainer.checkpoint(0);
    expect(maintainer.walBytes()).toBe(0);
    await maintainer.sync();
  }, 180_000);

  it("confirms durability with a PASSIVE checkpoint and reports change stamps", async () => {
    const root = tempRoot("sync");
    const store = await open(root);
    const other = await open(root);
    const before = store.stateStamp();
    const version = store.dataVersion();
    await other.put({ key: "s/1", value: 1, identity });
    expect(store.stateStamp()).not.toBe(before);
    expect(store.dataVersion()).not.toBe(version);
    await store.sync();
    expect(store.stats().checkpoints.passive).toBeGreaterThan(0);
    expect(store.checkpoint().checkpointed).toBe(store.checkpoint().log);
  });

  it("refuses network and virtual filesystems and UNC paths", () => {
    const mountinfo = [
      "22 1 0:21 / / rw,relatime shared:1 - zfs rpool/ROOT rw",
      "40 22 0:40 / /mnt/nfs rw,relatime shared:2 - nfs4 server:/export rw",
      "41 22 0:41 / /mnt/ssh\\040dir rw,relatime shared:3 - fuse.sshfs host:/ rw",
      "42 22 0:42 / /run/desktop rw,relatime shared:4 - virtiofs shared rw",
      "43 40 0:43 / /mnt/nfs/local rw,relatime shared:5 - ext4 /dev/sdb1 rw",
    ].join("\n");
    const linux = (root: string) => filesystemRefusal(root, { platform: "linux", mountinfo });
    expect(linux("/home/paul/mesh")).toBeUndefined();
    expect(linux("/mnt/nfs/mesh")).toMatch(/nfs4/);
    expect(linux("/mnt/nfs/local/mesh")).toBeUndefined();
    expect(linux("/mnt/ssh dir/mesh")).toMatch(/fuse\.sshfs/);
    expect(linux("/run/desktop/mnt/host/mesh")).toMatch(/virtiofs/);
    expect(linux("/mnt/nfsish/mesh")).toBeUndefined();
    const win = (root: string) => filesystemRefusal(root, { platform: "win32" });
    expect(win("\\\\server\\share\\mesh")).toMatch(/network path/);
    expect(win("//server/share/mesh")).toMatch(/network path/);
    expect(win("\\\\?\\UNC\\server\\share\\mesh")).toMatch(/network path/);
    expect(win("\\\\?\\C:\\mesh")).toBeUndefined();
    expect(win("C:\\Users\\paul\\mesh")).toBeUndefined();
    expect(filesystemRefusal(os.tmpdir())).toBeUndefined();
  });
});
