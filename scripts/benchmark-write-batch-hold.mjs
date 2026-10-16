#!/usr/bin/env node
// Synthetic file/SQLite microbenchmark; NEVER point a MeshStore at a live root.
// Build the candidate first, then run in a fresh Node process (not Bun):
// TMPDIR=/private/tmp node scripts/benchmark-write-batch-hold.mjs --import-root /candidate/repo --out .local/baseline.json
// --import-root accepts a repo root or its dist directory. The actual candidate MeshStore
// comes from dist/mesh.js; its real host-compaction helper is bundled from that repo's src.
// No timing gate: compare raw lock-held samples, not acquisition wait or command wall time.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { build } from "esbuild";

const usage = "node scripts/benchmark-write-batch-hold.mjs --import-root REPO_OR_DIST [--out FILE] [--actors 190] [--samples 200] [--warmup 20] [--hosts 32] [--lock-protocol 1] [--refresh-plan locked|outside|both] [--backend file|sqlite]";
const options = { actors: 190, samples: 200, warmup: 20, hosts: 32, lockProtocol: 1, refreshPlan: "locked", backend: "file" };
const numberFlags = { actors: "actors", samples: "samples", warmup: "warmup", hosts: "hosts", "lock-protocol": "lockProtocol" };
for (let i = 2; i < process.argv.length; i++) {
  const arg = process.argv[i];
  if (arg === "--help") { console.log(usage); process.exit(0); }
  const flag = arg.slice(2), value = process.argv[++i];
  if (!arg.startsWith("--") || value === undefined) throw new Error(usage);
  if (flag === "import-root") options.importRoot = path.resolve(value);
  else if (flag === "out") options.out = path.resolve(value);
  else if (flag === "refresh-plan") options.refreshPlan = value;
  else if (flag === "backend") options.backend = value;
  else if (numberFlags[flag]) {
    const n = Number(value);
    if (!Number.isSafeInteger(n) || n < (flag === "warmup" ? 0 : 1) || n > 10000) throw new Error(`invalid --${flag}`);
    options[numberFlags[flag]] = n;
  } else throw new Error(`unknown argument ${arg}\n${usage}`);
}
if (!options.importRoot || !process.env.TMPDIR) throw new Error(`--import-root and a private TMPDIR are required\n${usage}`);
if (![1, 2, 3].includes(options.lockProtocol)) throw new Error("--lock-protocol must be 1, 2 or 3");
if (!["locked", "outside", "both"].includes(options.refreshPlan)) throw new Error("--refresh-plan must be locked, outside or both");
if (!["file", "sqlite"].includes(options.backend)) throw new Error("--backend must be file or sqlite");
process.umask(0o022);
// Diagnostics capture environment exactly once at module import. Do not inherit live sinks.
process.env.PI_FABRIC_LOCK_STATS = "1";
const scratch = fs.mkdtempSync(path.join(fs.realpathSync(process.env.TMPDIR), "write-batch-hold-"));
process.env.PI_FABRIC_COMMIT_STATS = path.join(scratch, "commits.jsonl");
const repo = path.basename(options.importRoot) === "dist" ? path.dirname(options.importRoot) : options.importRoot;
const entry = path.join(repo, "dist/mesh.js");
const helperSource = path.join(repo, "src/topology/host-record-compaction.ts");
const helper = path.join(scratch, "host-record-compaction.mjs");
const recordsSource = path.join(repo, "src/topology/records.ts");
const migrationSource = path.join(repo, "src/mesh/backend-migration.ts");
await build({ stdin: { contents: `export { compactExpiredHostRecords } from ${JSON.stringify(helperSource)};\nexport { actorParticipantRecord } from ${JSON.stringify(recordsSource)};\n${options.backend === "sqlite" ? `export { importMeshState } from ${JSON.stringify(migrationSource)};` : ""}`, resolveDir: repo },
  outfile: helper, bundle: true, packages: "external", platform: "node", format: "esm", logLevel: "silent" });
const { MeshStore } = await import(pathToFileURL(entry).href);
const { compactExpiredHostRecords, actorParticipantRecord, importMeshState } = await import(pathToFileURL(helper).href);
const lockRegistry = globalThis[Symbol.for("pi-fabric.mesh.lock-stats")];
const commitRegistry = globalThis[Symbol.for("pi-fabric.mesh.commit-stats")];
assert(lockRegistry?.stats, "candidate must expose the existing L8 lock recorder");
if (options.backend === "file") assert(commitRegistry?.counter, "file candidate must expose the commit recorder");
const stats = lockRegistry.stats, counter = commitRegistry?.counter;
const original = { acquired: stats.acquired, failed: stats.failed, record: counter?.record };
let active;
stats.acquired = function (root, lockClass, waitMs, holdMs) {
  original.acquired.call(this, root, lockClass, waitMs, holdMs);
  if (active && root === active.root) active.holds.push({ lockClass, waitMs, holdMs });
};
stats.failed = function (root, lockClass, waitMs, tried) {
  original.failed.call(this, root, lockClass, waitMs, tried);
  if (active && root === active.root) active.failures.push({ lockClass, waitMs, tried });
};
if (counter) counter.record = function (bytes, keys) {
  original.record.call(this, bytes, keys);
  if (active) active.commits.push({ bytes, changedKeys: keys.length });
};

const hash = text => createHash("sha256").update(text).digest("hex");
const hostKey = id => `topology/hosts/${hash(id)}`;
const participantKey = id => `topology/participants/${hash(id)}`;
const pad = n => String(n).padStart(4, "0");
const identity = { id: "session:benchmark-owner", name: "benchmark-owner", kind: "main", sessionId: "benchmark-owner" };
const epoch = 1800000000000;
const actorValue = i => ({
  id: `actor-${pad(i)}`, name: `registry-actor-${pad(i)}`, rootId: identity.id,
  ownershipToken: `lineage-${pad(i)}`, ownerSessionId: identity.sessionId,
  project: { root: "/synthetic/benchmark/project", name: "benchmark" },
  instructionsDigest: hash(`private persona ${i}`), instructionsLength: 8192,
  status: "idle", runner: "pi", kernel: "typescript", events: ["mesh", "message"],
  topics: [`fleet.work.benchmark.${pad(i)}`, "fleet.work.benchmark.*"],
  delivery: "followUp", responseMode: "last", triggerTurn: true, coalesce: true, residency: "durable",
  model: "cliproxyapi/gpt-6.1-sol", thinking: "xhigh",
  binding: { scope: "session", sessionId: identity.sessionId, model: "cliproxyapi/gpt-6.1-sol", thinking: "xhigh", updatedAt: epoch },
  projectDefaults: { scope: "project", model: "cliproxyapi/gpt-6.1-sol", thinking: "xhigh" },
  tools: ["read", "bash", "edit", "write", "fabric_exec"], timeoutMs: 120000,
  filterSkipped: { count: 0, lastKey: null, lastTopic: null, lastAt: null }, requirements: [],
  queued: 0, messages: 24, createdAt: epoch, updatedAt: epoch,
  sessionFile: `/synthetic/benchmark/actors/actor-${pad(i)}/session.jsonl`,
  logDir: `/synthetic/benchmark/actors/actor-${pad(i)}/runs`,
});
const actorValues = Array.from({ length: options.actors }, (_, i) => actorValue(i));
// Public presence contains instruction digest/length and message COUNT, not private persona/rings
// (ActorManager.#info). Do not inflate shared state with a fictitious multi-MB actor registry.
const participants = actorValues.map(actor => actorParticipantRecord(actor, identity.id, identity.id, identity.id, identity.id));
const presenceOps = actorValues.map(value => ({ kind: "put", key: `actors/${identity.sessionId}/${value.id}`, value, identity }));
const participantOps = participants.map(value => ({ kind: "put", key: participantKey(value.id), value }));
const hostValue = (id, at) => ({ format: 1, livenessLeaseFiles: 1, id, rootId: id,
  identity: id === identity.id ? identity : { id, name: id, kind: "main" }, startedAt: epoch,
  incarnationToken: `incarnation-${id}`, updatedAt: at, expiresAt: at + 120000 });
const seedOps = [...presenceOps, ...participantOps,
  ...Array.from({ length: options.hosts }, (_, i) => {
    const id = i === 0 ? identity.id : `session:peer-${pad(i)}`;
    return { kind: "put", key: hostKey(id), value: now => hostValue(id, now) };
  })];
const percentile = (samples, p) => [...samples].sort((a, b) => a - b)[Math.max(0, Math.ceil(samples.length * p) - 1)];
const summary = samples => ({ n: samples.length, p50: percentile(samples, 0.50), p95: percentile(samples, 0.95),
  min: Math.min(...samples), max: Math.max(...samples), mean: samples.reduce((a, b) => a + b, 0) / samples.length });
const hostBefore = { loadavg: os.loadavg(), cpu: process.cpuUsage(), memory: process.memoryUsage() };
const results = [];
try {
  const plans = options.refreshPlan === "both" ? ["locked", "outside"] : [options.refreshPlan];
  const cases = [{ shape: "static-presence", plan: "none", outbox: false }, ...plans.flatMap(plan => [
    { shape: `refresh-${plan}-outbox`, plan, outbox: true },
    { shape: `refresh-${plan}-no-outbox`, plan, outbox: false },
  ])];
  for (const { shape, plan, outbox } of cases) {
    const root = path.join(scratch, shape);
    // Production SQLite refuses unimported roots. Use the real candidate migration,
    // outside measurement, not a fixture override of the backend's safety guard.
    if (options.backend === "sqlite") {
      fs.mkdirSync(root, { mode: 0o700 });
      await importMeshState(root);
    }
    const mesh = new MeshStore(root, 256 * 1024, 1000, { lockTimeoutMs: 120000, lockProtocol: options.lockProtocol, lockHeldEventMs: 0, stateBackend: options.backend });
    await mesh.writeBatch({ identity, ops: seedOps });
    const diagnostics = mesh.stateDiagnostics();
    assert.equal(mesh.stateBackend, options.backend, "backend fallback invalidates this measurement");
    assert.equal(diagnostics.kind, options.backend);
    assert.equal(diagnostics.fallback, undefined);
    const stateFile = path.join(root, "state.json");
    const stateBytesBefore = options.backend === "file" ? fs.statSync(stateFile).size : undefined;
    const initialEntries = mesh.listAll("", { fresh: true }); // Always outside measured custody.
    const logicalBytesBefore = Buffer.byteLength(JSON.stringify(initialEntries));
    const stampBefore = mesh.stateStamp();
    assert.equal(typeof stampBefore, "string");
    let previousStamp = stampBefore;
    const versions = new Map(initialEntries.map(entry => [entry.key, entry.version]));
    let lastResults;
    let outboxes = 0, prepares = 0, beforeCommits = 0, outsidePlans = 0;
    const rows = [];
    // The real #refresh's callback calls compactExpiredHostRecords and a functional host put;
    // fresh peers exercise its full namespace selection without changing the fixture's size.
    // The outbox function is intentionally cheap, like publication.committed(). Its detached
    // full-state snapshot is produced UNDER the lock on file; SQLite captures lazily
    // after COMMIT and may expose a later commit. Never impose file snapshot semantics on it.
    for (let index = 0; index < options.warmup + options.samples; index++) {
      const sample = { root, holds: [], failures: [], commits: [] };
      const ops = shape === "static-presence" ? presenceOps : [...presenceOps, ...participantOps,
        { kind: "put", key: hostKey(identity.id), value: now => hostValue(identity.id, now) }];
      let preparedDeletes = 0;
      const input = { identity, ops };
      if (plan === "locked") input.prepare = view => {
        prepares++;
        const deletions = compactExpiredHostRecords(view, root, identity.id);
        preparedDeletes = deletions.length;
        return deletions;
      };
      if (plan === "outside") {
        // Candidate caller shape: selection happens before writeBatch/custody. The
        // fixture has no expired hosts, so moving it never changes the operation set.
        const deletions = compactExpiredHostRecords({ listAll: prefix => mesh.listAll(prefix, { fresh: true }) }, root, identity.id);
        outsidePlans++;
        preparedDeletes = deletions.length;
        input.ops = [...ops, ...deletions];
        // Real admission/lease gates are deliberately not faked; only hook dispatch is
        // included. Parent must run the actual caller's behavioral gates separately.
        input.beforeCommit = () => { beforeCommits++; };
      }
      if (outbox) input.commitOutbox = effects => {
        outboxes++;
        assert.equal(effects.backend, options.backend);
        assert.deepEqual(effects.changed, ops.map(op => op.key));
        assert.equal(effects.results.length, ops.length);
        assert(effects.results.every(result => result.applied));
        const entry = effects.view.get(hostKey(identity.id));
        assert.equal(entry.value.id, identity.id);
        const result = effects.results.find(result => result.key === entry.key);
        if (options.backend === "file") assert.equal(entry.version, result.version);
        else assert(entry.version >= result.version, "SQLite outbox view may be newer, never older");
      };
      active = sample;
      const committed = await mesh.writeBatch(input);
      active = undefined;
      assert.equal(committed.length, ops.length);
      assert(committed.every(result => result.applied), "every planned put must commit");
      assert.deepEqual(committed.map(result => result.key), ops.map(op => op.key));
      const stamp = mesh.stateStamp();
      assert.equal(typeof stamp, "string");
      assert.notEqual(stamp, previousStamp, "committed state stamp must advance");
      previousStamp = stamp;
      const entries = new Map(mesh.listAll("", { fresh: true }).map(entry => [entry.key, entry]));
      assert.equal(entries.size, seedOps.length);
      for (const result of committed) {
        const entry = entries.get(result.key);
        assert(result.version > versions.get(result.key), "key version must advance");
        assert.equal(entry.version, result.version, "keyed read must agree with batch result");
        versions.set(result.key, result.version);
      }
      lastResults = committed;
      assert.equal(preparedDeletes, 0, "fixture must stay stable across samples");
      assert.equal(sample.holds.length, 1, "exactly one mesh-lock hold per writeBatch required");
      assert.equal(sample.holds[0].lockClass, "writeBatch");
      assert.equal(sample.failures.length, 0);
      if (options.backend === "file") {
        assert.equal(sample.commits.length, 1, "exactly one canonical state rewrite per writeBatch required");
        assert.equal(sample.commits[0].changedKeys, ops.length);
      } else assert.equal(sample.commits.length, 0, "SQLite must not report fictitious file rewrites");
      if (index >= options.warmup) rows.push({ index: index - options.warmup,
        ...sample.holds[0], ...sample.commits[0], ops: ops.length, stamp,
        resultsSha256: hash(JSON.stringify(committed)),
        minVersion: Math.min(...committed.map(result => result.version)),
        maxVersion: Math.max(...committed.map(result => result.version)) });
    }
    const keyedState = mesh.listAll("", { fresh: true });
    assert.equal(keyedState.length, seedOps.length);
    const finalByKey = new Map(keyedState.map(entry => [entry.key, entry]));
    for (const op of [...presenceOps, ...participantOps]) assert.deepEqual(finalByKey.get(op.key).value, op.value);
    for (const entry of keyedState.filter(entry => entry.key.startsWith("topology/hosts/"))) {
      assert.deepEqual(entry.value, hostValue(entry.value.id, entry.updatedAt));
    }
    if (options.backend === "file") {
      const canonical = JSON.parse(fs.readFileSync(stateFile, "utf8"));
      assert.equal(Object.keys(canonical.entries).length, seedOps.length);
    } else {
      const marker = JSON.parse(fs.readFileSync(stateFile, "utf8"));
      assert.equal(marker.format, "sqlite");
      assert.equal(marker.movedTo, "state.db");
      assert.equal(marker.entries, undefined, "SQLite state.json is only a migration marker, never canonical state");
    }
    assert.equal(prepares, plan === "locked" ? options.warmup + options.samples : 0);
    assert.equal(outsidePlans, plan === "outside" ? options.warmup + options.samples : 0);
    assert.equal(beforeCommits, plan === "outside" ? options.warmup + options.samples : 0);
    assert.equal(outboxes, outbox ? options.warmup + options.samples : 0);
    lockRegistry.flush();
    const recordedAcquisitions = fs.readdirSync(path.join(root, "lock-stats")).filter(name => name.endsWith(".json"))
      .flatMap(name => JSON.parse(fs.readFileSync(path.join(root, "lock-stats", name), "utf8")).minutes)
      .reduce((n, minute) => n + (minute.classes.writeBatch?.n ?? 0), 0);
    assert.equal(recordedAcquisitions, options.warmup + options.samples + 1, "L8 file count must agree (including seed)");
    const physicalBytes = diagnostics.database ? Object.fromEntries(["", "-wal", "-shm"].flatMap(suffix => {
      const file = diagnostics.database + suffix;
      return fs.existsSync(file) ? [[file, fs.statSync(file).size]] : [];
    })) : undefined;
    const result = { shape, refreshPlan: plan, backend: options.backend, diagnostics: mesh.stateDiagnostics(), root,
      entries: keyedState.length, logicalBytesBefore, logicalBytesAfter: Buffer.byteLength(JSON.stringify(keyedState)),
      logicalSizeEncoding: "UTF-8 JSON.stringify(listAll entries); off-lock, not canonical file or database bytes",
      stampBefore, stampAfter: previousStamp, keyedState, lastResults, physicalBytes,
      stateBytesBefore, stateBytesAfter: options.backend === "file" ? fs.statSync(stateFile).size : undefined,
      opsPerBatch: rows[0].ops, samples: rows.length, warmup: options.warmup, prepares, outsidePlans, beforeCommits, outboxes,
      holdMs: summary(rows.map(row => row.holdMs)), waitMs: summary(rows.map(row => row.waitMs)),
      ...(options.backend === "file" ? { rewrittenBytes: summary(rows.map(row => row.bytes)) } : {}), recordedAcquisitions, rows };
    results.push(result);
    console.log(`${shape}: backend=${options.backend} actors=${options.actors} entries=${result.entries} logicalBytes=${result.logicalBytesAfter} ops/batch=${result.opsPerBatch} n=${result.samples} hold p50=${result.holdMs.p50.toFixed(3)}ms p95=${result.holdMs.p95.toFixed(3)}ms wait p95=${result.waitMs.p95.toFixed(3)}ms`);
    mesh.closeState();
  }
  let head;
  try { head = execFileSync("git", ["-C", repo, "rev-parse", "HEAD"], { encoding: "utf8" }).trim(); } catch { head = "unavailable"; }
  const report = { benchmark: "write-batch-hold", schema: 2, recordedAt: new Date().toISOString(),
    candidate: { repo, head, entry, entrySha256: hash(fs.readFileSync(entry)), helperSource, helperSourceSha256: hash(fs.readFileSync(helperSource)), recordsSource, recordsSourceSha256: hash(fs.readFileSync(recordsSource)),
      ...(options.backend === "sqlite" ? { migrationSource, migrationSourceSha256: hash(fs.readFileSync(migrationSource)) } : {}) },
    harness: { file: fileURLToPath(import.meta.url), sha256: hash(fs.readFileSync(fileURLToPath(import.meta.url))) },
    command: [process.execPath, ...process.argv.slice(1)], environment: { TMPDIR: process.env.TMPDIR, NODE_OPTIONS: process.env.NODE_OPTIONS },
    host: { hostname: os.hostname(), platform: process.platform, arch: process.arch, node: process.version,
      cpus: os.availableParallelism(), before: hostBefore, after: { loadavg: os.loadavg(), cpu: process.cpuUsage(), memory: process.memoryUsage() } },
    options, scratch, fixture: { actors: options.actors, hosts: options.hosts,
      presenceValueBytes: summary(actorValues.map(value => Buffer.byteLength(JSON.stringify(value)))),
      participantValueBytes: summary(participants.map(value => Buffer.byteLength(JSON.stringify(value)))),
      actorInstructionsBytes: 8192, messageCountPerActor: 24, privateRegistryIncludedInSharedState: false },
    measurement: options.backend === "file"
      ? "Existing L8 acquired: post-pin operation through releaseOwned; excludes wait and off-lock preparation/validation. Real canonical rewrite counter. Raw samples, nearest-rank percentiles; seed/warmup excluded."
      : "Existing SQLite L8 acquired: post-BEGIN IMMEDIATE transaction body through COMMIT/ROLLBACK release; excludes wait and off-lock preparation/validation/outbox reads. No file rewrite metrics. Raw samples, nearest-rank percentiles; seed/warmup excluded.",
    limitations: ["Synthetic batch shapes, not end-to-end ParticipantDirectory/ActorManager timing.",
      "No registry/lease admission fences, assertHostLeaseOwner or participant-file copies; those paths are not modified.",
      "All actors idle but renewable; stopped-presence suppression is intentionally not benchmarked.",
      "Fresh host records exercise compaction selection, not expiry/deletion or lease-file recovery.",
      "outside plan mode models moving compaction selection out of custody with beforeCommit hook dispatch only, not the caller's real admission checks.",
      "No CPU burner or added contention fixture; naturally occurring shared-host load only.",
      "L8 waitMs includes the acquisition path; it is diagnostic only and never the hold metric."], results, validation: { ok: true } };
  if (options.out) {
    fs.mkdirSync(path.dirname(options.out), { recursive: true });
    fs.writeFileSync(options.out, JSON.stringify(report, null, 2) + "\n");
    console.log(`raw samples and metadata: ${options.out}`);
  }
  console.log(`private fixture retained: ${scratch}`);
} finally {
  active = undefined;
  stats.acquired = original.acquired;
  stats.failed = original.failed;
  if (counter) counter.record = original.record;
  lockRegistry.flush();
  lockRegistry.dispose();
  commitRegistry?.dispose();
}
