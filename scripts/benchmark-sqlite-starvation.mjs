#!/usr/bin/env node
// Non-reproducing lab harness: N real StoreBridgeSide.mirror processes + one SqliteStateStore waiter.
// Synthetic roots ONLY; not a production-like busy-writer starvation reproduction or fix.
// Node 24, run under the existing owned-job/CPU cap (e.g. .local/run-job.sh), never against a live mesh.
// Smoke: node scripts/benchmark-sqlite-starvation.mjs --n=2 --seconds=0.2 --budgetMs=200
// 10 s budget (lab): node scripts/benchmark-sqlite-starvation.mjs --n=8 --seconds=15 --budgetMs=10000
// Optional: --mode=both|probe-on|probe-off --label=main-before --out=/absolute/new-report.json
// probe-on is unmodified source. probe-off disables ONLY probeEligible while bundling: a controlled
// experiment, NOT an old release. Both modes repeat each peer's OWN unchanged, initially installed
// snapshot (fixed source clock, no synthetic holds). Timed offered workload is identical; achieved
// pass counts and scheduling/jitter are not deterministic. A waiter win means reproduction UNKNOWN.
// The single initial waiter may encounter zero active SQL competition. Zero hot SQL commits
// DURING its wait is not steady-state starvation exposure, regardless of total hot transactions.
// One watchdog bounds each mode; only owned children are killed/joined. Scratch is private and this
// script never deletes it, but wrappers may delete TMPDIR: only stdout / explicit --out are durable.
import assert from "node:assert/strict";
import { execFileSync, fork } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath, pathToFileURL } from "node:url";
const self = fileURLToPath(import.meta.url), repo = path.resolve(path.dirname(self), "..");
const args = Object.fromEntries(process.argv.slice(2).map(arg => {
  const m = /^--([A-Za-z]+)=(.+)$/.exec(arg); assert(m, `Invalid argument: ${arg}`); return [m[1], m[2]];
}));
const hash = value => createHash("sha256").update(value).digest("hex");
const now = () => Number(process.hrtime.bigint()) / 1e6;
const epoch = 1_900_000_000_000, identity = { id: "starvation-waiter", name: "waiter", kind: "agent" };
const receipt = { key: "receipt/waiter", value: { benchmark: "sqlite-starvation", serial: 0 } };
const fixture = index => {
  const name = `starvation-${index}`, id = `session:${name}`;
  const host = { format: 1, id, rootId: id, identity: { id, name, kind: "main", sessionId: name },
    startedAt: epoch - 10000, incarnationToken: `token-${index}`, updatedAt: epoch, expiresAt: epoch + 60000 };
  return { hosts: [{ record: host, expiresAt: host.expiresAt, observedAt: epoch }], participants: [{
    format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name, label: name,
    status: "idle", runner: "pi", transport: "host", capabilities: ["steer"], sessionId: name,
    startedAt: host.startedAt, updatedAt: epoch, controlProtocol: "v1" }], reserved: [] };
};
process.env.PI_FABRIC_LOCK_STATS = "0";
process.env.PI_FABRIC_MESH_CHECKPOINT_LOG = "off";
delete process.env.PI_FABRIC_COMMIT_STATS;
if (args.worker) {
  // Observe successful native SQL, including empty COMMITs and rollbacks; forward exactly once.
  // Unlike commit_no, this counts writer admission even for unchanged mirror passes.
  let active;
  const exec = DatabaseSync.prototype.exec, writers = new WeakSet();
  DatabaseSync.prototype.exec = function (sql) {
    const result = exec.call(this, sql);
    if (sql === "BEGIN IMMEDIATE") {
      writers.add(this);
      if (active) { active.transactions++; active.acquiredAt ??= now(); }
    }
    // stateView also uses BEGIN/COMMIT for READ snapshots: never count those as writers.
    if ((sql === "COMMIT" || sql === "ROLLBACK") && writers.delete(this) && active) {
      if (sql === "COMMIT") active.commitTimes.push(now()); else active.rollbacks++;
    }
    return result;
  };
  const { MeshStore, StoreBridgeSide, SqliteStateStore } = await import(pathToFileURL(args.module).href);
  const index = Number(args.worker), hot = index < Number(args.n), records = [];
  const store = hot ? new MeshStore(args.root, 65536, 1000,
    { stateBackend: "sqlite", lockTimeoutMs: Number(args.budgetMs), lockHeldEventMs: 0 })
    : await SqliteStateStore.open(args.root, 65536, 1000, { lockTimeoutMs: Number(args.budgetMs) });
  const close = () => hot ? store.closeState() : store.close();
  const snapshot = hot ? fixture(index) : undefined;
  const side = hot ? new StoreBridgeSide(store, `peer-${index}`, () => epoch) : undefined;
  if (hot) { assert.equal(store.stateBackend, "sqlite"); await side.mirror(snapshot, epoch); }
  process.send({ kind: "ready", index });
  process.once("message", async ({ stopAt }) => {
    const cpu = process.cpuUsage();
    try {
      do {
        const record = { requestedAt: now(), transactions: 0, commitTimes: [], rollbacks: 0 };
        active = record;
        try {
          if (hot) await side.mirror(snapshot, epoch);
          else record.receipt = await store.writeBatch({ identity, ops: [{ kind: "put", ...receipt }] });
          record.outcome = "ok";
        } catch (error) {
          if (error.code !== "FABRIC_MESH_LOCK_TIMEOUT") throw error;
          record.outcome = "timeout"; record.code = error.code; record.busyCode = error.busyCode;
        } finally { active = undefined; }
        record.finishedAt = now(); records.push(record);
        if (hot && records.length === 1) process.send({ kind: "hot", index });
        await new Promise(resolve => setImmediate(resolve)); // no think time or script-level retry
      } while (hot && now() < stopAt);
      const spent = process.cpuUsage(cpu);
      close();
      process.send({ kind: "report", index, records, cpuUs: spent.user + spent.system }, () => process.disconnect());
    } catch (error) {
      close(); process.exitCode = 1;
      process.send({ kind: "error", error: String(error.stack ?? error) }, () => process.disconnect());
    }
  });
} else {
  for (const key of Object.keys(args)) assert(["n", "seconds", "budgetMs", "mode", "label", "out"].includes(key), `Unknown option ${key}`);
  const n = Number(args.n ?? 8), seconds = Number(args.seconds ?? 15), budgetMs = Number(args.budgetMs ?? 10000);
  assert(Number.isSafeInteger(n) && n >= 1 && n <= 32, "n: 1..32 hot processes (+ one waiter)");
  assert(Number.isFinite(seconds) && seconds >= 0.05 && seconds <= 60, "seconds: 0.05..60");
  assert(Number.isSafeInteger(budgetMs) && budgetMs >= 10 && budgetMs <= 10000, "budgetMs: 10..10000");
  assert(["both", "probe-on", "probe-off"].includes(args.mode ?? "both"));
  if (args.out) { assert(path.isAbsolute(args.out), "out must be absolute"); assert(!fs.existsSync(args.out), "out must be a NEW file"); }
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "sqlite-starvation-"));
  fs.chmodSync(scratch, 0o700);
  console.error(`Private scratch (caller may remove TMPDIR): ${scratch}`);
  const head = execFileSync("git", ["rev-parse", "HEAD"], { cwd: repo, encoding: "utf8" }).trim();
  const { build, version: esbuild } = await import("esbuild");
  const predicate = "const probeEligible = (view: MeshBatchView | undefined): boolean => this.#hasCommittedPass && !halted() &&\n" +
    "      view !== undefined && view.listAll(BRIDGE_HANDOFF_PREFIX).length === 0 && !this.#outbox.hasUnconfirmed();";
  const replacement = "const probeEligible = (_view: MeshBatchView | undefined): boolean => false;";
  const workload = { n, seconds, budgetMs, clock: epoch, snapshots: Array.from({ length: n }, (_, i) => fixture(i)), receipt,
    start: "waiter after first completed hot mirror", cadence: "unchanged snapshot; setImmediate; timed offered load" };
  const workloadHash = hash(JSON.stringify(workload)), results = [];
  let graphHash;
  for (const mode of args.mode && args.mode !== "both" ? [args.mode] : ["probe-on", "probe-off"]) {
    const module = path.join(scratch, `${mode}.mjs`), root = path.join(scratch, mode), inputs = {};
    let matches = 0;
    await build({ stdin: { contents: 'export { MeshStore } from "./src/mesh/store.ts";\n' +
      'export { StoreBridgeSide } from "./src/mesh/bridge.ts";\nexport { SqliteStateStore } from "./src/mesh/state-sqlite.ts";',
      resolveDir: repo, loader: "ts" }, outfile: module, bundle: true, platform: "node", target: "node24", format: "esm", logLevel: "error",
      plugins: [{ name: "pinned-probe-only", setup(builder) {
        builder.onLoad({ filter: /\.(?:[cm]?[jt]s|json)$/ }, ({ path: file }) => {
          let contents = fs.readFileSync(file, "utf8"); inputs[path.relative(repo, file)] = hash(contents);
          if (file === path.join(repo, "src/mesh/bridge.ts")) {
            assert.equal(contents.split(predicate).length - 1, 1, "probeEligible must match EXACTLY ONCE"); matches++;
            if (mode === "probe-off") contents = contents.replace(predicate, replacement);
          }
          return { contents, loader: file.endsWith("json") ? "json" : file.endsWith("ts") ? "ts" : "js" };
        });
      } }] });
    assert.equal(matches, 1);
    const sourceGraphHash = hash(JSON.stringify(Object.entries(inputs).sort(([a], [b]) => a.localeCompare(b))));
    graphHash ??= sourceGraphHash; assert.equal(sourceGraphHash, graphHash, "source changed between modes");
    const { SqliteStateStore } = await import(pathToFileURL(module).href);
    fs.mkdirSync(root, { mode: 0o700 });
    SqliteStateStore.openSync(root, 65536, 1000, { initialize: "create" }).close();
    const children = [], exits = [], reports = new Map();
    let rejectRun, waiterStarted = false;
    const failure = new Promise((_, reject) => { rejectRun = reject; });
    const launch = index => new Promise(resolve => {
      const child = fork(self, [`--worker=${index}`, `--n=${n}`, `--module=${module}`, `--root=${root}`, `--budgetMs=${budgetMs}`],
        { cwd: repo, stdio: ["ignore", "ignore", "inherit", "ipc"] });
      children.push(child);
      exits.push(new Promise(done => {
        child.once("error", error => { rejectRun(error); done(); });
        child.once("close", (code, signal) => {
          done(); if (code !== 0 || !reports.has(index)) rejectRun(new Error(`Worker ${index}: ${code}/${signal}, report=${reports.has(index)}`));
        });
      }));
      child.on("message", message => {
        if (message.kind === "ready") resolve(child);
        if (message.kind === "error") rejectRun(new Error(message.error));
        if (message.kind === "report") reports.set(index, message);
        if (message.kind === "hot" && !waiterStarted) { waiterStarted = true; children[n].send({ stopAt: 0 }); }
      });
    });
    // IPC drives startup/progress; this single non-renewing timer is only the missed-event safety net.
    const watchdog = setTimeout(() => rejectRun(new Error("bounded mode watchdog expired")), (seconds + budgetMs / 1000 + 45) * 1000);
    try {
      // Install each peer's own snapshot without startup contention; keep that same side alive for repeats.
      for (let index = 0; index <= n; index++) await Promise.race([launch(index), failure]);
      const check = await SqliteStateStore.open(root, 65536, 1000);
      let before, initialCommit;
      const db = new DatabaseSync(path.join(root, "state.db"), { readOnly: true });
      const commitNo = () => Number(db.prepare("SELECT value FROM meta WHERE name='commit_no'").get().value);
      try {
        before = check.listAll(""); initialCommit = commitNo();
        for (const prefix of ["topology/hosts/", "topology/participants/"]) {
          const rows = before.filter(row => row.key.startsWith(prefix)); assert.equal(rows.length, n);
          for (let i = 0; i < n; i++) {
            const host = fixture(i).hosts[0].record, row = rows.find(row => row.key === prefix + hash(host.id));
            assert(row); assert.equal(row.value.id, host.id); assert.equal(row.value.remoteHost, `peer-${i}`);
            assert.deepEqual(row.updatedBy, host.identity);
            if (prefix.includes("participants")) assert.deepEqual(row.value, { ...fixture(i).participants[0], remoteHost: `peer-${i}` });
          }
        }
        const stopAt = now() + seconds * 1000;
        for (const child of children.slice(0, n)) child.send({ stopAt });
        await Promise.race([Promise.all(exits), failure]);
        assert(waiterStarted); assert.equal(reports.size, n + 1);
        const all = [...reports.values()].sort((a, b) => a.index - b.index), waiter = reports.get(n).records[0];
        assert.equal(reports.get(n).records.length, 1);
        const after = check.listAll("");
        assert.deepEqual(after.filter(row => row.key !== receipt.key), before, "mirror row/value/version/identity changed");
        const row = check.get(receipt.key), ok = waiter.outcome === "ok";
        if (ok) {
          assert.deepEqual(row.value, receipt.value); assert.deepEqual(row.updatedBy, identity);
          assert.equal(waiter.receipt.length, 1); assert.equal(waiter.receipt[0].applied, true);
          assert.equal(row.version, waiter.receipt[0].version);
        } else assert.equal(row, undefined, "timeout left a receipt");
        assert.equal(after.length, before.length + Number(ok));
        assert.equal(commitNo() - initialCommit, Number(ok), "unexpected changed-state commit");
        assert.equal(db.prepare("PRAGMA integrity_check").get().integrity_check, "ok");
        const end = waiter.acquiredAt ?? waiter.finishedAt;
        const hotRecords = all.filter(r => r.index < n).flatMap(r => r.records);
        const hotCommitsDuringWait = hotRecords.flatMap(r => r.commitTimes).filter(t => t >= waiter.requestedAt && t <= end).length;
        const hotPassesDuringWait = hotRecords.filter(r => r.outcome === "ok" && r.finishedAt >= waiter.requestedAt && r.finishedAt <= end).length;
        const perWriter = all.map(report => {
          const r = report.records, progress = [r[0].requestedAt, ...r.flatMap(x => x.commitTimes), r.at(-1).finishedAt];
          return { index: report.index, role: report.index < n ? "hot-mirror" : "waiter", passes: r.length,
            timeouts: r.filter(x => x.outcome === "timeout").length,
            transactions: r.reduce((sum, x) => sum + x.transactions, 0), sqlCommits: r.reduce((sum, x) => sum + x.commitTimes.length, 0),
            rollbacks: r.reduce((sum, x) => sum + x.rollbacks, 0), cpuUs: report.cpuUs,
            maxCallMs: r.reduce((max, x) => Math.max(max, x.finishedAt - x.requestedAt), 0),
            maxNoSqlCommitMs: progress.reduce((max, t, i) => Math.max(max, t - (progress[i - 1] ?? t)), 0) };
        });
        for (const r of perWriter) assert.equal(r.transactions, r.sqlCommits + r.rollbacks);
        if (mode === "probe-on") assert(perWriter.slice(0, n).every(r => r.transactions === 0), "probe-on unexpectedly acquired writer");
        else assert(perWriter.slice(0, n).every(r => r.transactions > 0), "probe-off did not exercise real writer admission");
        results.push({ mode, sourceGraphHash, moduleHash: hash(fs.readFileSync(module)), inputHashes: inputs,
          controlledPatch: mode === "probe-off" ? { predicate, replacement } : null, workloadHash,
          integrity: "PASS (exact rows/receipt + commit_no + PRAGMA; NOT fairness)", changedStateCommits: Number(ok),
          waiterOutcome: waiter.outcome, waiterWaitMs: end - waiter.requestedAt, waiterCallMs: waiter.finishedAt - waiter.requestedAt,
          hotCommitsDuringWait, hotPassesDuringWait, perWriter,
          reproduction: !ok && hotCommitsDuringWait > 0 ? "observed-waiter-timeout-with-hot-sql-commits" : "unknown", records: all });
      } finally { db.close(); check.close(); }
    } finally {
      clearTimeout(watchdog);
      for (const child of children) if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      await Promise.all(exits);
    }
  }
  const pins = Object.fromEntries(["package.json", "bun.lock", "scripts/benchmark-sqlite-starvation.mjs"].map(file => [file, hash(fs.readFileSync(path.join(repo, file)))]));
  const result = { schema: 1, benchmark: "StoreBridgeSide.mirror + SqliteStateStore.writeBatch", head, node: process.version,
    esbuild, platform: process.platform, label: args.label ?? "unlabelled", pins, workloadHash, workload, scratch,
    scratchLifetime: "not durable when caller removes TMPDIR", results };
  if (args.out) fs.writeFileSync(args.out, JSON.stringify(result, null, 2) + "\n", { mode: 0o600, flag: "wx" });
  console.log(JSON.stringify({ ...result, results: results.map(({ records, inputHashes, ...summary }) => ({ ...summary,
    recordsHash: hash(JSON.stringify(records)), inputHashes })) }, null, 2));
}
