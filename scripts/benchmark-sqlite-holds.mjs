#!/usr/bin/env node
// N=10k real short writer transactions. Pair orders alternate; no timer assertion in CI.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createJiti } from "jiti";
const jiti = createJiti(import.meta.url);
const { openNodeSqlite } = await jiti.import("../src/mesh/state-sqlite.ts");
const { recordSqliteHolds, sqliteHoldsPath } = await jiti.import("../src/mesh/sqlite-holds.ts");
const n = 10_000;
const pairs = Number(process.env.PAIRS ?? 15);
if (!Number.isSafeInteger(pairs) || pairs < 1) throw new Error("PAIRS must be a positive integer");
const mode = process.env.SYNCHRONOUS ?? "NORMAL";
if (!["NORMAL", "FULL"].includes(mode)) throw new Error("SYNCHRONOUS must be NORMAL or FULL");
const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-hold-bench-"));
const raw = openNodeSqlite(path.join(root, "state.db"));
try {
  raw.exec("PRAGMA journal_mode=WAL");
  raw.exec("PRAGMA synchronous=" + mode);
  raw.exec("PRAGMA wal_autocheckpoint=1000");
  raw.exec("CREATE TABLE kv(key INTEGER PRIMARY KEY, value INTEGER)");
  raw.exec("INSERT INTO kv VALUES(1, 0)");
  const on = recordSqliteHolds(raw, root, "benchmark", { thresholdMs: 2000 });
  const offStatement = raw.prepare("UPDATE kv SET value = value + 1 WHERE key = 1");
  const onStatement = on.prepare("UPDATE kv SET value = value + 1 WHERE key = 1");
  function run(db, statement, count) {
    const started = performance.now();
    for (let i = 0; i < count; i++) {
      db.exec("BEGIN IMMEDIATE");
      statement.run();
      db.exec("COMMIT");
    }
    return performance.now() - started;
  }
  for (let i = 0; i < 5; i++) { run(raw, offStatement, n); run(on, onStatement, n); }
  const samples = [];
  for (let i = 0; i < pairs; i++) {
    let offMs = 0, onMs = 0;
    for (let block = 0; block < n / 100; block++) {
      if ((i + block) % 2 === 0) { offMs += run(raw, offStatement, 100); onMs += run(on, onStatement, 100); }
      else { onMs += run(on, onStatement, 100); offMs += run(raw, offStatement, 100); }
    }
    samples.push({ offMs, onMs, differencePct: (onMs / offMs - 1) * 100 });
  }
  const median = values => values.toSorted((a, b) => a - b)[Math.floor(values.length / 2)];
  const offMs = median(samples.map(s => s.offMs)), onMs = median(samples.map(s => s.onMs));
  const actual = Number(raw.prepare("SELECT value FROM kv WHERE key = 1").get().value);
  if (actual !== (pairs + 5) * 2 * n) throw new Error("recorder changed transaction result");
  if (fs.existsSync(sqliteHoldsPath(root))) throw new Error("short transactions emitted I/O");
  console.log(JSON.stringify({ runtime: process.version, n, pairs, synchronous: mode, journalMode: "WAL",
    interleavedBlockSize: 100, offMedianMs: offMs, onMedianMs: onMs, differencePct: (onMs / offMs - 1) * 100,
    medianPairedDifferencePct: median(samples.map(s => s.differencePct)),
    samples, finalValue: actual, metricsLines: 0 }, null, 2));
} finally {
  raw.close();
  fs.rmSync(root, { recursive: true, force: true });
}
