// SQLite WAL baseline for smarty-dev#7504. node:sqlite, one worker thread and
// one connection per writer; writers serialize on the WAL write lock.
//   node sqlitebench.mjs load DB FULL|NORMAL row|tx3 CONC WARM_S DUR_S
//   node sqlitebench.mjs killwriter DB ROUND CONC   (child; prints acked keys)
//   node sqlitebench.mjs kill DB ROUNDS CONC         (parent; kill -9 + verify)
import { DatabaseSync } from "node:sqlite";
import { Worker, isMainThread, workerData, parentPort } from "node:worker_threads";
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { createInterface } from "node:readline";
import { writeSync } from "node:fs";

function open(db, sync) {
  const d = new DatabaseSync(db);
  d.exec(`PRAGMA busy_timeout=30000; PRAGMA journal_mode=WAL; PRAGMA synchronous=${sync};
          CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v BLOB NOT NULL)`);
  return d;
}

if (!isMainThread) {
  const { db, sync, kind, w, warm, dur, sab } = workerData;
  const flags = new Int32Array(sab); // [0]=phase 0 warm/1 window/2 after, [1]=stop
  const d = open(db, sync);
  const up = d.prepare("INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v");
  const begin = d.prepare("BEGIN IMMEDIATE"), commit = d.prepare("COMMIT");
  const v = randomBytes(200);
  if (kind === "kill") {
    // Unique keys; a key is reported ("acked") only after the autocommit returns.
    for (let n = 0; ; n++) {
      const k = `r${workerData.round}.w${w}.n${n}`, val = randomBytes(100);
      up.run(k, val);
      writeSync(1, `${k} ${val.toString("hex")}\n`);
    }
  }
  const lat = [];
  // Fresh value bytes per put: SQLite skips the page write (and the fsync) when an
  // upsert stores bytes identical to the old row, which would fake the FULL numbers.
  let ctr = 0;
  const val = () => (v.writeUInt32LE(ctr++ >>> 0, 0), v.writeUInt32LE(w, 4), v);
  const key = () => "k" + Math.floor(Math.random() * 10000);
  let busy = 0, done = 0;
  // Throughput counts ops that complete inside the window. Latency samples are all
  // ops that overlap the window, so a writer starved by the busy handler since
  // warm-up still shows its full wait in the tail.
  while (!Atomics.load(flags, 1)) {
    const ps = Atomics.load(flags, 0);
    const t0 = process.hrtime.bigint();
    try {
      if (kind === "row") up.run(key(), val());
      else { begin.run(); up.run(key(), val()); up.run(key(), val()); up.run(key(), val()); commit.run(); }
    } catch (e) {
      if (!/busy|locked/i.test(String(e.message))) throw e;
      busy++; if (d.isTransaction) d.exec("ROLLBACK"); continue;
    }
    const pe = Atomics.load(flags, 0);
    if (ps <= 1 && pe >= 1) lat.push(Number(process.hrtime.bigint() - t0) / 1e6);
    if (pe === 1) done++;
  }
  d.close();
  parentPort.postMessage({ lat: Float64Array.from(lat), busy, done });
} else {
  const [mode, ...a] = process.argv.slice(2);
  if (mode === "load") {
    const [db, sync, kind, c, warm, dur] = a;
    open(db, sync).close();
    const sab = new SharedArrayBuffer(8), flags = new Int32Array(sab);
    const done = [];
    for (let w = 0; w < +c; w++) {
      const wk = new Worker(new URL(import.meta.url), { workerData: { db, sync, kind, w, sab } });
      done.push(new Promise((res, rej) => { wk.on("message", res); wk.on("error", rej); }));
    }
    const sleep = (s) => new Promise((r) => setTimeout(r, s * 1000));
    await sleep(+warm);
    Atomics.store(flags, 0, 1);
    const t0 = performance.now();
    await sleep(+dur);
    Atomics.store(flags, 0, 2);
    const el = (performance.now() - t0) / 1000;
    Atomics.store(flags, 1, 1);
    const parts = await Promise.all(done);
    const per = parts.map((p) => p.done), ops = per.reduce((x, y) => x + y, 0), busy = parts.reduce((n, p) => n + p.busy, 0);
    parts.forEach((p, i) => (parts[i] = p.lat));
    const all = new Float64Array(parts.reduce((n, p) => n + p.length, 0));
    let o = 0; for (const p of parts) { all.set(p, o); o += p.length; }
    all.sort();
    const pct = (p) => all[Math.min(all.length - 1, Math.floor(p * all.length))] ?? 0;
    console.log(JSON.stringify({ backend: `sqlite-${sync}`, case: kind, c: +c, ops, secs: el,
      ops_s: ops / el, p50_ms: pct(0.5), p99_ms: pct(0.99), p999_ms: pct(0.999), max_ms: all[all.length - 1] ?? 0, per_writer_min: Math.min(...per), per_writer_max: Math.max(...per), busy_errors: busy }));
  } else if (mode === "load1c") {
    // One connection in one process; CONC async callers queue for it (how an
    // in-process store serializes writers). Latency = queue wait + commit.
    const [db, sync, kind, c, warm, dur] = a;
    const d = open(db, sync);
    const up = d.prepare("INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v");
    const v = randomBytes(200), key = () => "k" + Math.floor(Math.random() * 10000);
    let ctr = 0; const val = () => (v.writeUInt32LE(ctr++ >>> 0, 0), v); // see worker note
    let tail = Promise.resolve(), measuring = false, stop = false;
    const lat = [];
    const op = () => (tail = tail.then(() => new Promise((res) => setImmediate(() => {
      if (kind === "row") up.run(key(), val());
      else { d.exec("BEGIN IMMEDIATE"); up.run(key(), val()); up.run(key(), val()); up.run(key(), val()); d.exec("COMMIT"); }
      res();
    }))));
    const loops = [];
    for (let w = 0; w < +c; w++) loops.push((async () => {
      while (!stop) { const m = measuring, t0 = performance.now(); await op(); if (m) lat.push(performance.now() - t0); }
    })());
    const sleep = (s) => new Promise((r) => setTimeout(r, s * 1000));
    await sleep(+warm); measuring = true; const t0 = performance.now();
    await sleep(+dur); measuring = false; const el = (performance.now() - t0) / 1000;
    stop = true; await Promise.all(loops); d.close();
    const all = Float64Array.from(lat).sort();
    const pct = (p) => all[Math.min(all.length - 1, Math.floor(p * all.length))] ?? 0;
    console.log(JSON.stringify({ backend: `sqlite1c-${sync}`, case: kind, c: +c, ops: all.length, secs: el,
      ops_s: all.length / el, p50_ms: pct(0.5), p99_ms: pct(0.99), p999_ms: pct(0.999), max_ms: all[all.length - 1] ?? 0 }));
  } else if (mode === "killwriter") {
    // Single-process writer with CONC async loops would not overlap (sync API), so
    // the child runs CONC worker threads; each prints a key only after COMMIT returns.
    const [db, round, c] = a;
    open(db, "FULL").close();
    for (let w = 0; w < +c; w++)
      new Worker(new URL(import.meta.url), { workerData: { db, sync: "FULL", kind: "kill", w, round } });
  } else if (mode === "kill") {
    const [db, rounds, c] = a;
    const acked = new Map();
    for (let r = 1; r <= +rounds; r++) {
      const ch = spawn(process.execPath, [new URL(import.meta.url).pathname, "killwriter", db, String(r), c],
        { stdio: ["ignore", "pipe", "inherit"] });
      console.error(`sqlite writer pid ${ch.pid}`);
      let n = 0;
      const rl = createInterface({ input: ch.stdout });
      rl.on("line", (l) => { const i = l.indexOf(" "); if (i > 0) { acked.set(l.slice(0, i), l.slice(i + 1)); n++; } });
      const exited = new Promise((res) => ch.on("exit", res));
      const closed = new Promise((res) => rl.on("close", res));
      await new Promise((res) => setTimeout(res, 3000 + Math.random() * 4000));
      process.kill(ch.pid, "SIGKILL");
      await exited; await closed;
      const t0 = performance.now();
      const d = open(db, "FULL");
      const get = d.prepare("SELECT v FROM kv WHERE k=?");
      get.get("x");
      const ready = performance.now() - t0;
      let missing = 0, wrong = 0;
      for (const [k, v] of acked) {
        const row = get.get(k);
        if (!row) missing++; else if (Buffer.from(row.v).toString("hex") !== v) wrong++;
      }
      const ic = d.prepare("PRAGMA integrity_check").get();
      const rows = d.prepare("SELECT count(*) n FROM kv").get().n;
      d.close();
      console.log(JSON.stringify({ round: r, killed_pid: ch.pid, acked_this_round: n, acked_total: acked.size,
        missing, wrong_value: wrong, rows, integrity: Object.values(ic)[0], reopen_ready_ms: +ready.toFixed(2) }));
    }
  }
}
