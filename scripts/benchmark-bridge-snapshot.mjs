#!/usr/bin/env node
/**
 * Synthetic SQLite microbenchmark of ONE real StoreBridgeSide.subscribeChanges baseline.
 * No transport, live mesh, production data, or snapshot/fence/notifier replacement.
 *
 *   node scripts/benchmark-bridge-snapshot.mjs --before-ref=<commit> --out=<file>
 *   node scripts/benchmark-bridge-snapshot.mjs --before-ref=<commit> --before-only --out=<file>
 *
 * Defaults: candidate bridge from WORKTREE, 12 samples/variant/scenario, 4 warmup pairs.
 * --after-ref=<commit> compares committed bridge bytes instead. --scratch=<existing private dir>
 * retains the generated source/bundle/fixture; otherwise a new private OS temp dir is retained.
 * Run heavy invocations under systemd-run --user -p MemoryMax=8G and your owned-job wrapper.
 *
 * CPU boundary: process.cpuUsage immediately around the synchronous subscribeChanges call,
 * including identical REAL fs.watch setup, revision/directory stamps, capture(s), SQLite write
 * fence(s), parsing, sorting and hashes, plus any automatic GC/worker CPU during that interval.
 * Construction, unsubscribe, fixture setup, compilation, public projection cross-checks and
 * output serialization are OUTSIDE the interval. No explicit GC is requested.
 * Counters wrap real calls, returning their unmodified results. No timing pass/fail threshold.
 * The optional forced-rejection scenario calls real validation first, then rejects the first two
 * successful proofs. All three real write fences remain; this is NOT a concurrent-churn test.
 * Historical substitution changes ONLY bridge.ts; both variants use the same dependency graph.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { build } from "esbuild";
import { DatabaseSync } from "node:sqlite";

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const options = Object.fromEntries(process.argv.slice(2).map(arg => {
  assert(arg.startsWith("--"), `Expected --option: ${arg}`);
  const at = arg.indexOf("=");
  return at < 0 ? [arg.slice(2), true] : [arg.slice(2, at), arg.slice(at + 1)];
}));
for (const key of Object.keys(options)) assert(["before-ref", "after-ref", "before-only", "samples", "warmup", "scratch", "out"].includes(key), `Unknown option: ${key}`);
assert(typeof options["before-ref"] === "string", "--before-ref=<commit> is required");
assert(options["before-only"] === undefined || options["before-only"] === true, "--before-only takes no value");
const samples = Number(options.samples ?? 12), warmup = Number(options.warmup ?? 4);
assert(Number.isSafeInteger(samples) && samples >= 8, "At least 8 samples are required");
assert(Number.isSafeInteger(warmup) && warmup >= 2, "At least 2 warmup rounds are required");
const git = (...args) => execFileSync("git", args, { cwd: repo, encoding: "utf8", maxBuffer: 16 * 1024 * 1024 }).trim();
const sha256 = value => createHash("sha256").update(value).digest("hex");
const head = git("rev-parse", "HEAD");
const beforeRef = git("rev-parse", "--verify", `${options["before-ref"]}^{commit}`);
const afterRef = options["after-ref"] ?? "WORKTREE";
const bridgeFile = path.join(repo, "src/mesh/bridge.ts");
const historicalSource = ref => execFileSync("git", ["show", `${ref}:src/mesh/bridge.ts`], { cwd: repo, encoding: "utf8", maxBuffer: 16 * 1024 * 1024 });
const sources = { before: historicalSource(beforeRef) };
if (!options["before-only"]) sources.after = afterRef === "WORKTREE" ? fs.readFileSync(bridgeFile, "utf8") : historicalSource(git("rev-parse", "--verify", `${afterRef}^{commit}`));
// Refuse to silently attribute changed dependencies to this bridge-only comparison.
const changedSourcePaths = git("diff", "--name-only", beforeRef, "--", "src").split("\n").filter(Boolean);
assert(changedSourcePaths.every(file => file === "src/mesh/bridge.ts"), `Other source changes need a separate baseline: ${changedSourcePaths.join(", ")}`);
if (afterRef !== "WORKTREE" && !options["before-only"]) {
  const dependencyChanges = git("diff", "--name-only", beforeRef, afterRef, "--", "src").split("\n").filter(file => file && file !== "src/mesh/bridge.ts");
  assert.equal(dependencyChanges.length, 0, "Committed candidates must differ only in bridge.ts under src/");
}
const scratchParent = options.scratch ? fs.realpathSync(path.resolve(options.scratch)) : undefined;
if (scratchParent) assert.equal(fs.statSync(scratchParent).mode & 0o077, 0, "--scratch must be private");
const scratch = fs.mkdtempSync(path.join(scratchParent ?? os.tmpdir(), "bridge-snapshot-"));
fs.chmodSync(scratch, 0o700);
for (const [label, source] of Object.entries(sources)) fs.writeFileSync(path.join(scratch, `${label}-bridge.ts`), source, { mode: 0o600 });
const bundleFile = path.join(scratch, "benchmark-bundle.mjs");
const built = await build({
  stdin: { contents: Object.keys(sources).flatMap(label => [
    `export { StoreBridgeSide as ${label} } from "benchmark-bridge:${label}";`,
    `export { StoreBridgeSide as ${label}Inspect } from "benchmark-bridge:${label}-inspection";`,
  ]).join("\n") +
    '\nexport { MeshStore } from "./src/mesh/store.ts";\nexport { SqliteStateStore } from "./src/mesh/state-sqlite.ts";',
    resolveDir: repo, loader: "ts" },
  bundle: true, platform: "node", target: "node24", format: "esm", outfile: bundleFile, metafile: true, logLevel: "error",
  plugins: [{ name: "frozen-bridge-source", setup(build) {
    build.onResolve({ filter: /^benchmark-bridge:/ }, args => ({ path: args.path.slice("benchmark-bridge:".length), namespace: "frozen-bridge" }));
    build.onLoad({ filter: /.*/, namespace: "frozen-bridge" }, args => {
      const inspection = args.path.endsWith("-inspection");
      const label = inspection ? args.path.slice(0, -"-inspection".length) : args.path;
      let contents = sources[label];
      if (inspection) {
        // Only these verification-only exports are instrumented; timed exports stay byte-original.
        const anchor = "return { native, digest: contentDigest({ native: content, mirrored, relays }), topology: contentDigest({ topology, mirrored, relays }) };";
        assert.equal(contents.split(anchor).length, 2, `${label}: expected exactly one snapshot return anchor`);
        contents = contents.replace(anchor, "const result = { native, digest: contentDigest({ native: content, mirrored, relays }), topology: contentDigest({ topology, mirrored, relays }) };\n" +
          "globalThis.__bridgeBenchmarkInspect({ native, digestPreimage: { native: content, mirrored, relays }, topologyPreimage: { topology, mirrored, relays }, digest: result.digest, topology: result.topology });\nreturn result;");
      }
      return { contents, loader: "ts", resolveDir: path.dirname(bridgeFile) };
    });
  } }],
});
fs.writeFileSync(path.join(scratch, "metafile.json"), JSON.stringify(built.metafile, null, 2));
const implementation = await import(pathToFileURL(bundleFile).href);
const { MeshStore, SqliteStateStore } = implementation;
const HOSTS = "topology/hosts/", PARTICIPANTS = "topology/participants/", HANDOFFS = "topology/handoffs/";
const NOW = 1_900_000_000_000;
const keyFor = (prefix, id) => prefix + sha256(id);
const root = path.join(scratch, "mesh");
fs.mkdirSync(root, { mode: 0o700 });
// Explicit fresh-root initialization; never activate an existing mesh implicitly.
SqliteStateStore.openSync(root, 64 * 1024, 500, { initialize: "create" }).close();
const store = new MeshStore(root, 64 * 1024, 500, { stateBackend: "sqlite", lockHeldEventMs: 0 });
assert.equal(store.stateBackend, "sqlite", "Benchmark must not silently fall back to file state");
fs.mkdirSync(path.join(root, "participants"), { mode: 0o700 });
const leaseDir = path.join(root, "host-leases");
fs.mkdirSync(leaseDir, { mode: 0o700 });
const entries = [], hostRows = [];
const groups = [{ label: "native", count: 180 }, { label: "this-link", count: 168, remoteHost: "link-a" }, { label: "other-link", count: 170, remoteHost: "link-b" }];
for (const group of groups) for (let index = 0; index < group.count; index += 1) {
  const sessionId = `synthetic-${group.label}-${String(index).padStart(4, "0")}`;
  const id = `session:${sessionId}`;
  const identity = { id, name: `synthetic-${group.label}-${index}`, kind: "main", sessionId };
  // Some other-link rows have an onward hop; the rest are direct relay candidates.
  const remote = group.remoteHost ? { remoteHost: group.remoteHost, ...(group.label === "other-link" && index % 5 === 0 ? { bridgeVia: "link-c" } : {}) } : {};
  const host = { format: 1, id, rootId: id, identity, startedAt: NOW - 3_600_000, updatedAt: NOW - 1_000,
    expiresAt: NOW + (group.remoteHost ? 10_000 : 60_000), incarnationToken: `synthetic-incarnation-${sessionId}`, ...remote };
  hostRows.push(host);
  entries.push({ key: keyFor(HOSTS, id), value: host, identity });
  entries.push({ key: keyFor(PARTICIPANTS, id), identity,
    value: { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
      name: identity.name, label: `root-${index}`, status: index % 3 === 0 ? "working" : "idle", runner: "pi", transport: "process",
      capabilities: ["steer", "followUp", "status"], sessionId, startedAt: host.startedAt, updatedAt: NOW - 1_000,
      controlProtocol: "v1", ...remote } });
}
for (let index = 0; index < 257; index += 1) {
  const owner = hostRows[index % hostRows.length];
  const id = `agent:synthetic-${String(index).padStart(4, "0")}`;
  entries.push({ key: keyFor(PARTICIPANTS, id), identity: owner.identity,
    value: { format: 1, id, kind: index % 2 ? "agent" : "actor", rootId: owner.rootId, ownerHostId: owner.id, ownerIdentityId: owner.identity.id,
      name: `synthetic-worker-${index}`, label: `worker-${index}`, status: index % 2 ? "working" : "idle", runner: "pi", transport: "process",
      capabilities: ["status"], startedAt: NOW - 60_000, updatedAt: NOW - 1_000,
      ...(owner.remoteHost ? { remoteHost: owner.remoteHost } : {}) } });
}
// Exact 0.94 MB of value JSON under the two measured prefixes, using synthetic descriptions.
for (const entry of entries) entry.value.description = "";
let padding = 940_000 - entries.reduce((sum, entry) => sum + Buffer.byteLength(JSON.stringify(entry.value)), 0);
assert(padding >= 0);
const sentence = "Synthetic benchmark activity metadata; no real session content. ";
for (let index = 0; index < entries.length; index += 1) {
  const length = Math.floor(padding / (entries.length - index));
  entries[index].value.description = sentence.repeat(Math.ceil(length / sentence.length)).slice(0, length);
  padding -= length;
}
await store.writeBatch({ identity: { id: "benchmark", name: "benchmark", kind: "agent" },
  ops: entries.map(entry => ({ kind: "put", ...entry })) });
// Native root participant-file duplicates exercise the real merge path as well.
for (const entry of entries.filter(entry => entry.key.startsWith(PARTICIPANTS) && entry.value.kind === "root" && entry.value.remoteHost === undefined)) {
  const persisted = store.get(entry.key, { fresh: true });
  fs.writeFileSync(path.join(root, "participants", `${entry.key.slice(PARTICIPANTS.length)}.json`), JSON.stringify({ format: 1, ...persisted }), { mode: 0o600 });
}
for (let index = 0; index < 1_068; index += 1) {
  const host = hostRows[index];
  const id = host?.id ?? `session:synthetic-lease-only-${String(index).padStart(4, "0")}`;
  const lease = { format: 1, id, rootId: id, identityId: id, updatedAt: NOW - 500, expiresAt: NOW + 12_000,
    startedAt: host?.startedAt ?? NOW - 60_000, incarnationToken: host?.incarnationToken ?? `synthetic-lease-${index}` };
  fs.writeFileSync(path.join(leaseDir, `${sha256(id)}.json`), JSON.stringify(lease), { mode: 0o600 });
}
const diagnostic = new DatabaseSync(path.join(root, "state.db"), { readOnly: true });
const sqlCounts = diagnostic.prepare("SELECT CASE WHEN key LIKE 'topology/hosts/%' THEN 'hosts' ELSE 'participants' END AS prefix, count(*) AS rows, sum(length(CAST(value AS BLOB))) AS valueBytes FROM kv WHERE key LIKE 'topology/hosts/%' OR key LIKE 'topology/participants/%' GROUP BY prefix ORDER BY prefix").all();
const fixture = { groups, rows: Object.fromEntries(sqlCounts.map(row => [row.prefix, Number(row.rows)])),
  valueJsonBytes: sqlCounts.reduce((sum, row) => sum + Number(row.valueBytes), 0),
  leaseFiles: fs.readdirSync(leaseDir).filter(name => name.endsWith(".json")).length,
  participantFiles: fs.readdirSync(path.join(root, "participants")).length,
  databaseBytes: fs.statSync(path.join(root, "state.db")).size,
  diagnosticCacheSize: diagnostic.prepare("PRAGMA cache_size").get(),
  queryPlan: diagnostic.prepare("EXPLAIN QUERY PLAN SELECT key,value FROM kv WHERE key >= ? AND key < ?").all(HOSTS, "topology/hosts0") };
diagnostic.close();
assert.deepEqual(fixture.rows, { hosts: 518, participants: 775 });
assert.equal(fixture.valueJsonBytes, 940_000);
assert.equal(fixture.leaseFiles, 1_068);

let active;
const methods = [];
const wrap = (object, name, interceptor) => {
  const original = object[name];
  methods.push(() => { object[name] = original; });
  object[name] = function (...args) { return interceptor.call(this, original, args); };
};
for (const method of ["listAll", "listAllShared"]) wrap(store, method, function (original, args) {
  const result = original.apply(this, args);
  if (active) {
    const prefix = args[0] ?? "";
    const counter = active.reads[`${method}:${prefix}`] ??= { calls: 0, rows: 0 };
    counter.calls += 1;
    counter.rows += result.length;
  }
  return result;
});
wrap(store, "stateRevision", function (original, args) {
  const revision = original.apply(this, args);
  if (active) active.revisions.push(revision);
  return revision;
});
wrap(store, "withStateWriteFence", function (original, args) {
  if (active) active.fences += 1;
  return original.apply(this, args);
});
wrap(store, "stateUnchangedSince", function (original, args) {
  const actual = original.apply(this, args);
  if (active) {
    assert.deepEqual(args[1], [HOSTS, PARTICIPANTS, HANDOFFS]);
    active.validations.push({ revision: args[0], actual });
    if (active.forceRemaining > 0) { active.forceRemaining -= 1; active.forcedRejections += 1; return false; }
  }
  return actual;
});
wrap(fs, "statSync", function (original, args) {
  if (active && typeof args[0] === "string") {
    const target = args[0];
    if (target.startsWith(leaseDir + path.sep) && target.endsWith(".json")) active.leaseFileStats += 1;
    else if (target === leaseDir) active.leaseDirectoryStats += 1;
  }
  return original.apply(this, args);
});
const sides = Object.fromEntries(Object.keys(sources).map(label => [label, new implementation[label](store, "link-a", () => NOW)]));
const inspectSides = Object.fromEntries(Object.keys(sources).map(label => [label, new implementation[`${label}Inspect`](store, "link-a", () => NOW)]));
const scenarios = ["single-capture", "two-forced-rejections"];
const rows = [];
const measure = (label, scenario, round, measured) => {
  const counters = { reads: {}, revisions: [], fences: 0, validations: [], forcedRejections: 0,
    forceRemaining: scenario === "two-forced-rejections" ? 2 : 0, leaseFileStats: 0, leaseDirectoryStats: 0 };
  const failures = [], notices = [];
  let unsubscribe;
  active = counters;
  const start = process.cpuUsage();
  let used;
  try {
    unsubscribe = sides[label].subscribeChanges(kind => notices.push(kind), error => failures.push(String(error)));
    used = process.cpuUsage(start);
  } finally {
    active = undefined;
    unsubscribe?.(); // intentionally excluded; watcher setup is intentionally included
  }
  assert.equal(typeof unsubscribe, "function");
  assert.deepEqual(failures, []);
  assert.deepEqual(notices, []);
  const captures = scenario === "single-capture" ? 1 : 3;
  assert.equal(counters.fences, captures, "Real fence count changed");
  assert.equal(counters.revisions.length, Math.min(captures, 2));
  assert.equal(counters.validations.length, Math.min(captures, 2));
  assert(counters.revisions.every(value => typeof value === "string" && !value.startsWith("unreadable:")));
  assert(counters.validations.every(value => value.actual === true), "Unexpected actual churn in isolated fixture");
  assert.equal(counters.forcedRejections, captures - 1);
  assert.equal(counters.forceRemaining, 0);
  if (label === "before") {
    assert.equal(counters.reads[`listAll:${HOSTS}`]?.calls, 3 * captures);
    assert.equal(counters.reads[`listAll:${PARTICIPANTS}`]?.calls, 3 * captures);
    assert.equal(counters.leaseFileStats, 2 * fixture.leaseFiles * captures);
  }
  if (measured) rows.push({ label, scenario, round, cpuMs: (used.user + used.system) / 1_000,
    userMs: used.user / 1_000, systemMs: used.system / 1_000, ...counters });
};
let projectionCheck;
const snapshotByteChecks = {};
try {
  // Exact preimage/native comparisons are verification-only: no collector or replacement return
  // runs in a timed export. Both versions still execute real revisions, file stamps and fences.
  for (const scenario of scenarios) {
    const inspected = {};
    for (const [label, side] of Object.entries(inspectSides)) {
      const captures = [];
      globalThis.__bridgeBenchmarkInspect = value => captures.push({ native: JSON.stringify(value.native),
        digestPreimage: JSON.stringify(value.digestPreimage), topologyPreimage: JSON.stringify(value.topologyPreimage),
        digest: value.digest, topology: value.topology });
      active = { reads: {}, revisions: [], fences: 0, validations: [], forcedRejections: 0,
        forceRemaining: scenario === "two-forced-rejections" ? 2 : 0, leaseFileStats: 0, leaseDirectoryStats: 0 };
      const failures = [];
      let unsubscribe;
      try { unsubscribe = side.subscribeChanges(() => { throw new Error("Unexpected inspection notice"); }, error => failures.push(String(error))); }
      finally { active = undefined; unsubscribe?.(); delete globalThis.__bridgeBenchmarkInspect; }
      assert.deepEqual(failures, []);
      assert.equal(captures.length, scenario === "single-capture" ? 1 : 3);
      inspected[label] = captures;
      fs.writeFileSync(path.join(scratch, `${label}-${scenario}-accepted-preimages.json`), JSON.stringify(captures.at(-1), null, 2), { mode: 0o600 });
    }
    if (sides.after) {
      assert.deepEqual(inspected.before, inspected.after, "Snapshot native/digest/topology preimages differ byte-for-byte");
    }
    const accepted = inspected.before.at(-1);
    snapshotByteChecks[scenario] = { compared: Boolean(sides.after), capturesPerVariant: inspected.before.length,
      comparison: "exact JSON string equality for native and digest/topology preimages for every attempt, including accepted final capture",
      accepted: Object.fromEntries(Object.entries(accepted).map(([key, text]) => [key, { bytes: Buffer.byteLength(text), sha256: sha256(text) }])) };
  }
  for (let round = 0; round < warmup; round += 1) for (const scenario of scenarios) {
    const labels = Object.keys(sides);
    if (round % 2) labels.reverse();
    for (const label of labels) measure(label, scenario, round, false);
  }
  // Public native and direct-relay projections are also cross-checked outside the timing window.
  // Repair/churn semantics still belong to the separate regression suite, not this timing claim.
  const projections = {};
  for (const [label, side] of Object.entries(sides)) {
    const native = await side.presence(), relay = await side.relayPresence();
    assert.equal(native.hosts.length, 180);
    assert.equal(native.participants.length, 180);
    assert.equal(relay.hosts.length, 136);
    assert.equal(relay.participants.length, 136);
    projections[label] = { native: sha256(JSON.stringify(native)), relay: sha256(JSON.stringify(relay)) };
  }
  if (sides.after) assert.deepEqual(projections.before, projections.after, "Public projections differ");
  projectionCheck = projections;
  for (let round = 0; round < samples; round += 1) {
    const scenarioOrder = round % 2 ? [...scenarios].reverse() : scenarios;
    for (const scenario of scenarioOrder) {
      const labels = Object.keys(sides);
      if (round % 2) labels.reverse();
      for (const label of labels) measure(label, scenario, round, true);
    }
  }
} finally {
  for (const restore of methods.reverse()) restore();
  store.closeState();
}
const stats = values => {
  const sorted = [...values].sort((a, b) => a - b);
  const quantile = p => sorted[Math.max(0, Math.ceil(p * sorted.length) - 1)];
  return { min: sorted[0], p25: quantile(.25), median: sorted.length % 2 ? sorted[(sorted.length - 1) / 2] : (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2,
    p75: quantile(.75), max: sorted.at(-1) };
};
const summary = {};
for (const scenario of scenarios) {
  summary[scenario] = {};
  for (const label of Object.keys(sides)) {
    const mine = rows.filter(row => row.label === label && row.scenario === scenario);
    const countRecords = mine.map(({ reads, revisions, fences, validations, forcedRejections, leaseFileStats, leaseDirectoryStats }) =>
      ({ reads, revisions, fences, validations, forcedRejections, leaseFileStats, leaseDirectoryStats }));
    assert(countRecords.every(row => JSON.stringify(row) === JSON.stringify(countRecords[0])), "Structural counts vary across samples");
    summary[scenario][label] = { samples: mine.length, cpuMs: stats(mine.map(row => row.cpuMs)),
      userMs: stats(mine.map(row => row.userMs)), systemMs: stats(mine.map(row => row.systemMs)), counts: countRecords[0] };
  }
  if (sides.after) summary[scenario].medianCpuReductionPercent = 100 * (1 - summary[scenario].after.cpuMs.median / summary[scenario].before.cpuMs.median);
}
const sourcePins = Object.fromEntries(Object.entries(sources).map(([label, source]) => [label, {
  ref: label === "before" ? beforeRef : afterRef === "WORKTREE" ? "WORKTREE" : git("rev-parse", "--verify", `${afterRef}^{commit}`), sha256: sha256(source),
}]));
const result = { version: 1, benchmark: "real-subscribeChanges-sqlite-baseline", collectedAt: new Date().toISOString(), head,
  runtime: { node: process.version, platform: process.platform, arch: process.arch, gcExposed: typeof global.gc === "function" },
  sourcePins, changedSourcePaths, scriptSha256: sha256(fs.readFileSync(fileURLToPath(import.meta.url))),
  bundleSha256: sha256(fs.readFileSync(bundleFile)), warmupRoundsPerScenario: warmup, samplesPerVariantPerScenario: samples,
  measurement: { metric: "process.cpuUsage user+system microseconds / 1000", boundary: "immediately around synchronous subscribeChanges; real watch setup and capture/fences included; unsubscribe excluded",
    excluded: ["compilation", "fixture creation", "side construction", "public projection cross-check", "unsubscribe", "serialization"],
    interleaving: "round parity alternates before/after order; scenario order also alternates", forcedRejections: "real unchangedSince executes first; first two successful proofs then forced false; third capture inside real SQLite fence",
    limitations: ["synthetic warm fixture, not production", "one entry baseline plus real watch setup, not repeated notifications or RPCs", "forced rejection is not real concurrent churn", "historical bridge only; shared dependency graph", "no wall-time threshold or production CPU claim", "repair and actual concurrent churn semantics delegated to regression suite"],
    byteVerification: "separate inspection-only exports capture the exact snapshot return preimages; timed exports use unmodified source with no collector" },
  fixture, projectionCheck, snapshotByteChecks, summary, rows, evidence: { scratch, bundle: bundleFile, metafile: path.join(scratch, "metafile.json") } };
const text = JSON.stringify(result, null, 2) + "\n";
if (options.out) fs.writeFileSync(path.resolve(options.out), text, { mode: 0o600 });
console.log(JSON.stringify({ head, sourcePins, fixture, summary, evidence: result.evidence }, null, 2));
