import { spawn, type ChildProcess, type ChildProcessWithoutNullStreams } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { resolveMeshStateSource, rollbackMeshState } from "../src/mesh/backend-migration.js";
import { MeshStateRetiredError, MeshStateWalCapError, SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#6477 security pass P1 (comment 6075855599): show the WAL bound under readers that never let go.
// Case A pins one read transaction across the 64 MiB emergency threshold; Case B runs constant short readers.

type Database = { exec(sql: string): void; close(): void; prepare(sql: string): { get(...params: unknown[]): unknown } };
const { DatabaseSync } = process.getBuiltinModule("node:sqlite") as { DatabaseSync: new (file: string, options?: object) => Database };

const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const MiB = 1024 * 1024;
const roots: string[] = [];
const sqlite: SqliteStateStore[] = [];
const databases: Database[] = [];
const children: ChildProcess[] = [];

const tempRoot = (label: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-walpin-${label}-`));
  roots.push(root);
  return root;
};
const openStore = async (root: string, options: Parameters<typeof SqliteStateStore.open>[3] = {}): Promise<SqliteStateStore> => {
  const store = await SqliteStateStore.open(root, 8 * MiB, 1_000, { initialize: "create", ...options });
  sqlite.push(store);
  return store;
};
const raw = (root: string): Database => {
  const database = new DatabaseSync(path.join(root, "state.db"), { timeout: 0 });
  databases.push(database);
  return database;
};
const walBytes = (root: string): number => fs.statSync(path.join(root, "state.db-wal"), { throwIfNoEntry: false })?.size ?? 0;
const progress = (line: string): void => { if (process.env.WALPIN_PROGRESS) fs.appendFileSync(process.env.WALPIN_PROGRESS, `${new Date().toISOString()} ${line}\n`); };
const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
const pad = "x".repeat(64 * 1024);
// Case A thresholds. Production: emergency 64 MiB, run to 72 MiB (WALPIN_FULL=1, ~60-90 s on a loaded host; above
// the threshold every commit pays the ~400 ms TRUNCATE budget). Default: the same path scaled to 16/18 MiB so the
// file stays inside the per-file time budget. The code path is identical; only emergencyCheckpointBytes differs.
const FULL = process.env.WALPIN_FULL === "1";
const EMERGENCY = (FULL ? 64 : 16) * MiB;
const TARGET = (FULL ? 72 : 18) * MiB;
const KEYS = 16;
const mib = (bytes: number): number => Math.round((bytes / MiB) * 10) / 10;
const ms = (value: number): number => Math.round(value * 10) / 10;
const p99 = (values: number[]): number => {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * 0.99) - 1)]!;
};

afterEach(() => {
  for (const child of children.splice(0)) try { child.kill("SIGKILL"); } catch { /* gone */ }
  for (const database of databases.splice(0)) try { database.close(); } catch { /* closed */ }
  for (const store of sqlite.splice(0)) try { store.close(); } catch { /* closed */ }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// pi-fabric#694 P1 1 and 2: a small cap so each case runs in seconds. One commit of `pad` is ~70-100 KiB of WAL.
const SMALL_CAP = 4 * MiB;
type Outcome = { ok: true } | { ok: false; code?: string | undefined; message?: string };
const attempt = async (write: () => Promise<unknown>): Promise<Outcome> => {
  try { await write(); return { ok: true }; }
  catch (error) { return { ok: false, code: (error as { code?: string }).code, message: (error as Error).message }; }
};
const pin = (root: string): Database => {
  const reader = raw(root);
  reader.exec("BEGIN");
  reader.prepare("SELECT count(*) AS n FROM kv").get(); // takes the read mark: the snapshot stays pinned
  return reader;
};

// A SqliteStateStore in a second process (production default open: an imported root), driven over stdin.
const STORE_CHILD = `
import { createJiti } from "jiti";
import readline from "node:readline";
import { pathToFileURL } from "node:url";
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const { SqliteStateStore } = await jiti.import("./src/mesh/state-sqlite.ts");
const [root, cap] = process.argv.slice(-2);
const store = await SqliteStateStore.open(root, 8 * 1024 * 1024, 1000, { walHardCapBytes: Number(cap) });
const identity = { id: "child", name: "child", kind: "agent" };
process.stdout.write("ready\\n");
for await (const line of readline.createInterface({ input: process.stdin })) {
  const [key, n, pad] = JSON.parse(line);
  try { await store.put({ key, value: { n, pad }, identity }); process.stdout.write(JSON.stringify({ ok: true }) + "\\n"); }
  catch (error) { process.stdout.write(JSON.stringify({ ok: false, code: error.code, message: error.message }) + "\\n"); }
}
store.close();
`;
const startStoreChild = async (root: string, cap: number): Promise<{ put: (key: string, n: number) => Promise<Outcome>; stop: () => void }> => {
  const child: ChildProcessWithoutNullStreams = spawn(process.execPath, ["--disable-warning=ExperimentalWarning", "--input-type=module", "-e", STORE_CHILD, root, String(cap)],
    { cwd: process.cwd(), stdio: ["pipe", "pipe", "pipe"] });
  children.push(child);
  let buffer = "";
  let stderr = "";
  const waiting: Array<(line: string) => void> = [];
  const lines: string[] = [];
  child.stderr.on("data", (chunk: Buffer) => { stderr += chunk.toString(); });
  child.stdout.on("data", (chunk: Buffer) => {
    buffer += chunk.toString();
    for (let at = buffer.indexOf("\n"); at >= 0; at = buffer.indexOf("\n")) {
      const line = buffer.slice(0, at);
      buffer = buffer.slice(at + 1);
      const next = waiting.shift();
      if (next) next(line); else lines.push(line);
    }
  });
  const next = (): Promise<string> => new Promise((resolve, reject) => {
    const queued = lines.shift();
    if (queued !== undefined) { resolve(queued); return; }
    const timer = setTimeout(() => reject(new Error(`store child silent: ${stderr}`)), 30_000);
    waiting.push((line) => { clearTimeout(timer); resolve(line); });
  });
  expect(await next()).toBe("ready");
  return {
    put: async (key, n) => { child.stdin.write(`${JSON.stringify([key, n, pad])}\n`); return JSON.parse(await next()) as Outcome; },
    stop: () => { child.stdin.end(); },
  };
};

describe("SQLite WAL hard cap admission across stores and the rollback path (pi-fabric#694 P1 1 and 2)", () => {
  it("rejects a walHardCapBytes that is not a finite number above 0 at open, before any side effect", async () => {
    for (const bad of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
      const root = tempRoot("badcap");
      await expect(openStore(root, { walHardCapBytes: bad })).rejects.toThrow(TypeError);
      expect(() => SqliteStateStore.openSync(root, 8 * MiB, 1_000, { initialize: "create", walHardCapBytes: bad })).toThrow(TypeError);
      expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
    }
  });

  it("multi-store boundary: after the first refusal every store refuses, a child process included; the WAL stays within one commit per store", async () => {
    const root = tempRoot("multi");
    const a = await openStore(root, { walHardCapBytes: SMALL_CAP });
    const b = await openStore(root, { walHardCapBytes: SMALL_CAP });
    for (let i = 0; i < KEYS; i += 1) await a.put({ key: `k${i}`, value: { n: i, pad }, identity });
    const child = await startStoreChild(root, SMALL_CAP);
    const writers: Array<(n: number) => Promise<Outcome>> = [
      (n) => attempt(() => a.put({ key: `k${n % KEYS}`, value: { n, pad }, identity })),
      (n) => attempt(() => b.put({ key: `k${n % KEYS}`, value: { n, pad }, identity })),
      (n) => child.put(`k${n % KEYS}`, n),
    ];
    pin(root);

    let n = 0;
    let maxCommit = 0;
    let maxWal = walBytes(root);
    let first: { writer: number; outcome: Outcome } | undefined;
    let admittedOverCap = 0; // writes that committed although the shared WAL was already above the cap before them
    const began = performance.now();
    while (!first && performance.now() - began < 50_000) {
      const writer = n % writers.length;
      const before = walBytes(root);
      const outcome = await writers[writer]!(n);
      const after = walBytes(root);
      maxWal = Math.max(maxWal, after);
      if (outcome.ok) {
        maxCommit = Math.max(maxCommit, after - before);
        if (before > SMALL_CAP) admittedOverCap += 1;
      } else first = { writer, outcome };
      n += 1;
    }
    expect(first?.outcome).toMatchObject({ ok: false, code: "FABRIC_MESH_STATE_WAL_CAP" });
    const walAtRefusal = walBytes(root);
    expect(walAtRefusal).toBeGreaterThan(SMALL_CAP);
    // Admission reads the shared WAL: the first write by ANY store after the crossing is refused (the old
    // per-store latch let each store that had not committed past the cap yet write once more).
    expect(admittedOverCap).toBe(0);
    // Every store, the in-process ones and the child, refuses from now on, and nothing lands.
    for (let round = 0; round < 3; round += 1) {
      for (const [index, write] of writers.entries()) {
        const outcome = await write(n++);
        expect(outcome, `writer ${index} round ${round}`).toMatchObject({ ok: false, code: "FABRIC_MESH_STATE_WAL_CAP" });
        maxWal = Math.max(maxWal, walBytes(root));
      }
    }
    expect(walBytes(root)).toBe(walAtRefusal);
    expect(maxCommit).toBeGreaterThan(0);
    expect(maxWal).toBeLessThanOrEqual(SMALL_CAP + writers.length * maxCommit);
    child.stop();
  }, 90_000);

  it("restart: a NEW store over an already over-cap WAL refuses its first write (no unguarded write)", async () => {
    const root = tempRoot("restart");
    const first = await openStore(root, { walHardCapBytes: SMALL_CAP });
    for (let i = 0; i < KEYS; i += 1) await first.put({ key: `k${i}`, value: { n: i, pad }, identity });
    pin(root);
    let n = KEYS;
    let refused: Outcome = { ok: true };
    const began = performance.now();
    while (refused.ok && performance.now() - began < 50_000) refused = await attempt(() => first.put({ key: `k${n % KEYS}`, value: { n: n++, pad }, identity }));
    expect(refused).toMatchObject({ ok: false, code: "FABRIC_MESH_STATE_WAL_CAP" });
    first.close();
    const wal = walBytes(root);
    expect(wal).toBeGreaterThan(SMALL_CAP);

    const restarted = await openStore(root, { walHardCapBytes: SMALL_CAP });
    expect(await attempt(() => restarted.put({ key: "after-restart", value: { pad }, identity })))
      .toMatchObject({ ok: false, code: "FABRIC_MESH_STATE_WAL_CAP" });
    expect(walBytes(root)).toBe(wal);
    expect(restarted.get("after-restart")).toBeUndefined();
  }, 90_000);

  it("rollback while capped: the operator rollback (fabric-mesh-backend rollback) succeeds with the reader still pinned", async () => {
    const root = tempRoot("rollback");
    const store = await openStore(root, { walHardCapBytes: SMALL_CAP });
    const last = new Map<string, number>();
    let n = 0;
    for (; n < KEYS; n += 1) { await store.put({ key: `k${n}`, value: { n, pad }, identity }); last.set(`k${n}`, n); }
    const reader = pin(root);
    let refused: Outcome = { ok: true };
    const began = performance.now();
    while (performance.now() - began < 50_000) {
      const key = `k${n % KEYS}`;
      refused = await attempt(() => store.put({ key, value: { n, pad }, identity }));
      if (!refused.ok) break;
      last.set(key, n);
      n += 1;
    }
    expect(refused).toMatchObject({ ok: false, code: "FABRIC_MESH_STATE_WAL_CAP" });
    expect(walBytes(root)).toBeGreaterThan(SMALL_CAP);

    // The real rollback path (mesh-backend-cli `rollback` calls rollbackMeshState), still pinned and capped.
    const rolled = await rollbackMeshState(root);
    expect(rolled).toMatchObject({ backend: "file" });
    expect(resolveMeshStateSource(root).source).toBe("file");
    const files = new MeshStore(root, 8 * MiB, 1_000);
    for (const [key, value] of last) expect((files.get(key, { fresh: true })?.value as { n: number }).n).toBe(value); // every committed key
    reader.exec("COMMIT"); // release the reader
    await expect(store.put({ key: "late", value: 1, identity })).rejects.toThrow(MeshStateRetiredError); // the old store fails closed
    await files.put({ key: "file/after", value: 1, identity });
    expect(files.get("file/after", { fresh: true })?.value).toBe(1);
  }, 90_000);
});

describe("SQLite WAL bound under pinned and constant readers (smarty-dev#6477 P1)", () => {
  it("Case A: a pinned reader past the 64 MiB emergency threshold; release truncates, no lost write, no long stall", async () => {
    const root = tempRoot("pinned");
    const store = await openStore(root, { emergencyCheckpointBytes: EMERGENCY });
    const last = new Map<string, number>();
    let n = 0;
    const put = async (): Promise<number> => {
      const key = `k${n % KEYS}`;
      const started = performance.now();
      await store.put({ key, value: { n, pad }, identity });
      const elapsed = performance.now() - started;
      last.set(key, n);
      n += 1;
      return elapsed;
    };
    progress("A open");
    for (let i = 0; i < KEYS; i += 1) await put();
    progress("A seeded");

    const reader = raw(root);
    reader.exec("BEGIN");
    reader.prepare("SELECT count(*) AS n FROM kv").get(); // takes the read mark: the snapshot stays pinned

    const began = performance.now();
    const all: number[] = [];
    const above: number[] = [];
    const curve: Array<[number, number, number]> = [];
    let wal = walBytes(root);
    while (wal <= TARGET && performance.now() - began < 150_000) {
      const elapsed = await put();
      all.push(elapsed);
      wal = walBytes(root);
      if (wal > EMERGENCY) above.push(elapsed);
      if (n % 100 === 0) { curve.push([n, mib(wal), Math.round(performance.now() - began)]); progress(`A n=${n} wal=${mib(wal)}`); }
    }
    const pinnedStats = store.stats().checkpoints;
    const pinnedWal = wal;

    reader.exec("COMMIT"); // release the pin
    const released = performance.now();
    const after: number[] = [];
    let commitsAfter = 0;
    while (performance.now() - released < 3_000) {
      after.push(await put());
      commitsAfter += 1;
      if (commitsAfter >= 3 && walBytes(root) < 8 * MiB) break;
      await sleep(50);
    }
    const finalWal = walBytes(root);
    const stats = store.stats();
    const metrics = {
      case: "A-pinned-reader",
      emergencyMiB: mib(EMERGENCY), targetMiB: mib(TARGET),
      commitsWhilePinned: all.length,
      pinnedMs: Math.round(released - began),
      walWhenReleasedMiB: mib(pinnedWal),
      latencyMs: { max: ms(Math.max(...all)), p99: ms(p99(all)), commits: all.length },
      latencyAboveEmergencyMs: { max: ms(Math.max(0, ...above)), p99: ms(p99(above)), commits: above.length },
      afterRelease: { commits: commitsAfter, maxMs: ms(Math.max(...after)), msToBound: Math.round(performance.now() - released), walMiB: mib(finalWal) },
      checkpointsWhilePinned: pinnedStats,
      checkpointsFinal: stats.checkpoints,
      maxWalMiB: mib(stats.maxWalBytes),
      walCurve: curve,
    };
    console.log(`WALPIN ${JSON.stringify(metrics)}`);
    progress(`WALPIN ${JSON.stringify(metrics)}`);

    expect(pinnedWal).toBeGreaterThan(TARGET); // the pin defeats every checkpoint: the WAL only grows
    expect(pinnedStats.emergency).toBeGreaterThan(0); // the emergency path ran while pinned
    expect(finalWal).toBeLessThan(8 * MiB); // released: the WAL is truncated
    for (const [key, value] of last) expect((store.get(key)?.value as { n: number }).n).toBe(value); // no lost write
    expect(Math.max(...all, ...after)).toBeLessThan(2_000); // no indefinite writer stall
  }, 240_000);

  it("Case C: the WAL hard cap refuses writes while pinned, keeps reads, and recovers on the next write after release", async () => {
    // Production-size: cap 80 MiB over the 64 MiB emergency path (WALPIN_FULL=1). Default: cap 16 MiB, below the
    // default 64 MiB emergency threshold, so the refusal is reached without paying the per-commit TRUNCATE budget.
    const cap = (FULL ? 80 : 16) * MiB;
    const root = tempRoot("cap");
    const store = await openStore(root, { walHardCapBytes: cap, ...(FULL ? { emergencyCheckpointBytes: EMERGENCY } : {}) });
    const last = new Map<string, number>();
    let n = 0;
    const put = async (): Promise<unknown> => {
      const key = `k${n % KEYS}`;
      try { await store.put({ key, value: { n, pad }, identity }); last.set(key, n); return undefined; }
      catch (error) { return error; }
      finally { n += 1; }
    };
    for (let i = 0; i < KEYS; i += 1) expect(await put()).toBeUndefined();
    const reader = raw(root);
    reader.exec("BEGIN");
    reader.prepare("SELECT count(*) AS n FROM kv").get();

    const began = performance.now();
    let refusal: unknown;
    let commitsWhilePinned = 0;
    let maxMs = 0;
    while (performance.now() - began < 150_000) {
      const started = performance.now();
      refusal = await put();
      maxMs = Math.max(maxMs, performance.now() - started);
      if (refusal !== undefined) break;
      commitsWhilePinned += 1;
    }
    const walAtRefusal = walBytes(root);
    expect(refusal).toBeInstanceOf(MeshStateWalCapError);
    expect((refusal as MeshStateWalCapError).code).toBe("FABRIC_MESH_STATE_WAL_CAP");
    expect((refusal as Error).message).toContain(String(cap));
    expect((refusal as Error).message).toContain("a reader is pinning the WAL; restart it or roll back");
    // The report names the pinning reader: this process holds the raw read transaction.
    if (process.platform === "linux") expect((refusal as MeshStateWalCapError).readers).toContain(`pid ${process.pid} `);
    const refusedAgain = await put(); // still pinned: still refused, the WAL does not grow
    expect(refusedAgain).toBeInstanceOf(MeshStateWalCapError);
    expect(walBytes(root)).toBe(walAtRefusal);
    for (const [key, value] of last) expect((store.get(key)?.value as { n: number }).n).toBe(value); // reads keep working

    reader.exec("COMMIT");
    const released = performance.now();
    const recovered = await put(); // the first write after release truncates and commits
    const recoveryMs = performance.now() - released;
    expect(recovered).toBeUndefined();
    const walAfter = walBytes(root);
    const stats = store.stats();
    const metrics = { case: "C-wal-hard-cap", capMiB: mib(cap), commitsWhilePinned, walAtRefusalMiB: mib(walAtRefusal),
      maxCommitMs: ms(maxMs), refusal: (refusal as Error).message, recoveryMs: ms(recoveryMs), walAfterRecoveryMiB: mib(walAfter),
      checkpoints: stats.checkpoints };
    console.log(`WALPIN ${JSON.stringify(metrics)}`);
    progress(`WALPIN ${JSON.stringify(metrics)}`);
    expect(walAtRefusal).toBeGreaterThan(cap);
    expect(walAfter).toBeLessThan(8 * MiB);
    for (const [key, value] of last) expect((store.get(key)?.value as { n: number }).n).toBe(value); // no lost write
    expect(maxMs).toBeLessThan(2_000);
  }, 240_000);

  it("Case B: four processes of constant short readers keep the WAL under the emergency threshold", async () => {
    const root = tempRoot("readers");
    const store = await openStore(root);
    let n = 0;
    for (let i = 0; i < KEYS; i += 1) { await store.put({ key: `k${i}`, value: { n: n++, pad }, identity }); }
    const fixture = path.join(root, "reader.mjs");
    fs.writeFileSync(fixture, [
      "const { DatabaseSync } = process.getBuiltinModule('node:sqlite');",
      "const db = new DatabaseSync(process.argv[2], { timeout: 5 });",
      "const read = db.prepare('SELECT length(value) AS n FROM kv WHERE key = ?');",
      "let reads = 0, busy = 0;",
      "process.on('SIGTERM', () => { process.stdout.write(JSON.stringify({ reads, busy })); process.exit(0); });",
      "process.stdout.write('ready\\n');",
      `for (;;) { try { read.get('k' + (reads % ${KEYS})); reads++; } catch { busy++; } if ((reads + busy) % 2000 === 0) await new Promise((r) => setImmediate(r)); }`,
    ].join("\n"));
    const outputs: string[] = ["", "", "", ""];
    const exits: Array<Promise<void>> = [];
    const ready: Array<Promise<void>> = [];
    for (let i = 0; i < 4; i += 1) {
      const child = spawn(process.execPath, ["--no-warnings", fixture, path.join(root, "state.db")], { stdio: ["ignore", "pipe", "inherit"] });
      children.push(child);
      ready.push(new Promise((resolve) => child.stdout!.once("data", () => resolve())));
      child.stdout!.on("data", (chunk: Buffer) => { outputs[i] += chunk.toString(); });
      exits.push(new Promise((resolve) => child.once("exit", () => resolve())));
    }
    await Promise.all(ready);

    const began = performance.now();
    let maxWal = 0;
    let maxMs = 0;
    let commits = 0;
    const latencies: number[] = [];
    while (performance.now() - began < 20_000) {
      const started = performance.now();
      await store.put({ key: `k${n % KEYS}`, value: { n, pad }, identity });
      const elapsed = performance.now() - started;
      latencies.push(elapsed);
      maxMs = Math.max(maxMs, elapsed);
      n += 1;
      commits += 1;
      maxWal = Math.max(maxWal, walBytes(root));
      if (commits % 32 === 0) await sleep(0); // lets the deferred reset run between bursts
    }
    for (const child of children) child.kill("SIGTERM");
    await Promise.all(exits);
    children.splice(0);
    const readers = outputs.map((text) => { try { return JSON.parse(text.replace(/^ready\n/, "")); } catch { return text; } });
    const stats = store.stats();
    const metrics = {
      case: "B-constant-short-readers",
      commits,
      ms: Math.round(performance.now() - began),
      walMaxMiB: mib(maxWal),
      storeMaxWalMiB: mib(stats.maxWalBytes),
      latencyMs: { max: ms(maxMs), p99: ms(p99(latencies)) },
      checkpoints: stats.checkpoints,
      readers,
    };
    console.log(`WALPIN ${JSON.stringify(metrics)}`);
    progress(`WALPIN ${JSON.stringify(metrics)}`);

    expect(maxWal).toBeLessThan(64 * MiB);
    // Reported, not asserted: under constant readers the one-shot reset is mostly busy; the bound comes from
    // SQLite restarting the WAL from its start once PASSIVE backfill completes (the file stops growing).
    expect(stats.checkpoints.emergency).toBe(0);
    expect(stats.checkpoints.walResets + stats.checkpoints.walResetBusy).toBeGreaterThan(0);
    expect(maxMs).toBeLessThan(2_000);
  }, 90_000);
});
