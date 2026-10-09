import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";
import type { MeshIdentity } from "../src/mesh/store.js";

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
