#!/usr/bin/env node
/**
 * Real archive catch-up timing, work and async-responsiveness probe. No production state.
 *
 *   node scripts/benchmark-archive-catchup.mjs [--samples 7] [--profile acceptance|busy] [--min-speedup 10] [--out /absolute/result.json]
 *
 * Default acceptance profile mirrors the 4,096-event / 241-file test fixture, including its
 * >3 MiB topic file. The separately declared busy profile uses 12,032 events / 241 files and
 * a ~67 MiB busy topic. Always report a failing acceptance measurement, not just the busy one.
 *
 * Builds an isolated source bundle outside dist/. The final product build remains a separate gate.
 * Every timing pair runs in a fresh Node process, reusing only the persisted range index and OS
 * page cache. Both readers get identical input and query; the baseline is the literal old reader,
 * not a slower substitute. One untimed warmup per arm warms V8 and the filesystem identically.
 * Reports the first pre-warmup reopened-index read separately. No instrumentation in timed arms.
 * Routine CI should run mesh-archive-catchup.test.ts's deterministic work bounds, not timing gates.
 * Fixture and bundle scratch are retained for inspection; this script never cleans anything up.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const script = fileURLToPath(import.meta.url);
const repo = path.resolve(path.dirname(script), "..");
const all = () => true;
const missing = error => error?.code === "ENOENT";
const json = file => {
  try { return JSON.parse(fs.readFileSync(file, "utf8")); }
  catch (error) { if (missing(error)) return undefined; throw error; }
};
const numeric = directory => {
  try { return fs.readdirSync(directory).filter(name => /^\d+$/.test(name)).sort(); }
  catch (error) { if (missing(error)) return []; throw error; }
};
const days = dir => numeric(dir).flatMap(year => numeric(path.join(dir, year))
  .flatMap(month => numeric(path.join(dir, year, month)).map(day => `${year}/${month}/${day}`)));
const archiveFileName = topic => `${topic.replaceAll("/", "%2F").replaceAll(":", "%3A")}.jsonl`;

// Literal pre-change readAfter from 98e903559b7a6e048393a7d94f464b3f34f5705b,
// with its private helpers inlined. This fixture uses short topics, not hashed names.
function legacyReadAfter(dir, after, through, matches, limit, topic) {
  const found = [];
  for (const day of days(dir)) {
    const aborted = json(path.join(dir, day, "ABORTED.json"));
    const directory = path.join(dir, day);
    const seal = json(path.join(directory, "SEAL.json"));
    if (seal && Math.max(0, ...Object.values(seal.files).map(file => file.lastSequence)) <= after) continue;
    const names = topic !== undefined ? [archiveFileName(topic)] : fs.readdirSync(directory).filter(name => name.endsWith(".jsonl"));
    const dayEvents = [];
    for (const name of names) {
      if (seal?.files[name] && seal.files[name].lastSequence <= after) continue;
      let text;
      try { text = fs.readFileSync(path.join(directory, name), "utf8"); }
      catch (error) { if (missing(error)) continue; throw error; }
      const lines = text.split("\n");
      lines.pop();
      for (const line of lines.filter(Boolean)) {
        let event;
        try {
          const parsed = JSON.parse(line);
          event = typeof parsed.sequence === "number" && typeof parsed.id === "string" ? parsed : undefined;
        } catch { event = undefined; }
        if (event && event.sequence > after && event.sequence <= through && aborted?.[event.sequence] !== event.id && matches(event)) dayEvents.push(event);
      }
    }
    found.push(...dayEvents.sort((left, right) => left.sequence - right.sequence));
    if (found.length >= limit) break;
  }
  return found.slice(0, limit);
}
const digest = events => {
  const hash = createHash("sha256");
  for (const event of events) hash.update(JSON.stringify(event) + "\n");
  return hash.digest("hex");
};
const same = (actual, expected) => {
  assert.deepEqual(actual.map(event => event.sequence), expected.map(event => event.sequence));
  assert.equal(digest(actual), digest(expected), "Full serialized event bytes differ from legacy");
};
const timed = operation => {
  const cpu = process.cpuUsage(), started = performance.now();
  const events = operation();
  const wallMs = performance.now() - started;
  const used = process.cpuUsage(cpu);
  return { wallMs, cpuMs: (used.user + used.system) / 1000, count: events.length, digest: digest(events) };
};

function work(operation) {
  const originals = new Map(), handles = new Map();
  let parsedEvents = 0, archiveBytesRead = 0;
  const wrap = (name, replacement) => { originals.set(name, fs[name]); fs[name] = replacement(fs[name]); };
  const parse = JSON.parse;
  JSON.parse = (...args) => {
    const value = parse(...args);
    if (value && typeof value === "object" && "sequence" in value && "id" in value && "topic" in value) parsedEvents++;
    return value;
  };
  wrap("openSync", original => (...args) => { const fd = original(...args); handles.set(fd, String(args[0])); return fd; });
  wrap("closeSync", original => fd => { handles.delete(fd); return original(fd); });
  wrap("readFileSync", original => (...args) => {
    const value = original(...args), file = typeof args[0] === "number" ? handles.get(args[0]) : String(args[0]);
    if (file?.endsWith(".jsonl")) archiveBytesRead += Buffer.byteLength(value);
    return value;
  });
  wrap("readSync", original => (...args) => {
    const count = original(...args);
    if (handles.get(args[0])?.endsWith(".jsonl")) archiveBytesRead += count;
    return count;
  });
  try { return { events: operation(), metrics: () => ({ parsedEvents, archiveBytesRead }) }; }
  finally { JSON.parse = parse; for (const [name, original] of originals) fs[name] = original; }
}

// Native child workers contain only one pair, so reopening cannot be a shared module-cache hit.
if (process.argv[2] === "--worker") {
  const f = json(process.argv[3]);
  const { MeshArchive } = await import(pathToFileURL(f.bundle).href);
  const old = () => legacyReadAfter(f.dir, f.after, f.through, all, f.limit);
  const candidate = () => new MeshArchive(f.dir, f.root).readAfter(f.after, f.through, all, f.limit);
  const firstReopened = timed(candidate);
  same(candidate(), old());
  old(); candidate(); // One real untimed warmup per arm; no artificial sleeps or baseline work.
  let baseline, indexed;
  if (process.argv[4] === "candidate-first") { indexed = timed(candidate); baseline = timed(old); }
  else { baseline = timed(old); indexed = timed(candidate); }
  assert.equal(indexed.digest, baseline.digest);
  const oldWork = work(old), newWork = work(candidate);
  same(newWork.events, oldWork.events);
  process.stdout.write(JSON.stringify({ firstReopened, baseline, indexed, work: { baseline: oldWork.metrics(), indexed: newWork.metrics() } }));
} else {
  const args = process.argv.slice(2);
  const options = {};
  for (let index = 0; index < args.length; index += 2) {
    assert(["--samples", "--profile", "--min-speedup", "--out"].includes(args[index]), `Unknown option: ${args[index]}`);
    assert(args[index + 1] && !args[index + 1].startsWith("--"), `Missing ${args[index]}`);
    options[args[index]] = args[index + 1];
  }
  const samples = Number(options["--samples"] ?? 7), minSpeedup = Number(options["--min-speedup"] ?? 10);
  const profile = options["--profile"] ?? "acceptance";
  assert(["acceptance", "busy"].includes(profile), "--profile must be acceptance or busy");
  assert(Number.isSafeInteger(samples) && samples >= 3 && samples <= 31, "--samples must be 3..31");
  assert(Number.isFinite(minSpeedup) && minSpeedup > 0, "--min-speedup must be positive");
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "archive-catchup-benchmark-"));
  const bundle = path.join(scratch, "candidate.mjs");
  const { build } = await import("esbuild");
  await build({ stdin: { contents: 'export { MeshArchive } from "./src/mesh/archive.ts";\n' +
    'export { MeshStore } from "./src/mesh/store.ts";\nexport { StoreBridgeSide } from "./src/mesh/bridge.ts";',
    resolveDir: repo, loader: "ts" }, outfile: bundle, bundle: true, platform: "node", target: "node22", format: "esm",
    banner: { js: 'import { createRequire as __benchmarkCreateRequire } from "node:module"; const require = __benchmarkCreateRequire(import.meta.url);' },
    logLevel: "error" });
  const { MeshArchive, MeshStore, StoreBridgeSide } = await import(pathToFileURL(bundle).href);
  const from = { id: "session:archive-benchmark", name: "archive-benchmark", kind: "main" };
  const makeFixture = label => {
    const base = path.join(scratch, label), root = path.join(base, "mesh"), dir = path.join(base, "archive");
    fs.mkdirSync(root, { recursive: true }); fs.mkdirSync(dir);
    let sequence = 0, bytes = 0, files = 0;
    const events = [];
    // No seals: this is legacy unindexed history. The default is the acceptance fixture,
    // not a workload enlarged after observing a disappointing timing ratio.
    const largeText = profile === "acceptance" ? "large archived work result / é 🛰️ ".repeat(400)
      : "Synthetic archived work result é 🛰️ / ".repeat(190);
    const hotCount = profile === "acceptance" ? 256 : 8192;
    for (let day = 0; day < 16; day++) {
      const grouped = new Map();
      for (let index = 0; index < (day === 0 ? hotCount : 256); index++) {
        sequence++;
        const topic = day === 0 ? "fleet.work.large" : `fleet.work.topic${index % 16}`;
        const value = { id: `event-${sequence}`, sequence, topic, kind: "message", from, to: "session:receiver",
          createdAt: Date.UTC(2026, 0, day + 1), text: day === 0 ? largeText : `work result ${sequence}`,
          data: { ordinal: sequence, nested: [null, false, "é / 中文 / 🛰️", { escaped: "\n\t\"\\" }],
            ...(sequence % 3 === 0 ? { bridge: { from: "hub", id: `origin-${sequence}` } } : {}) } };
        events.push(value);
        const lines = grouped.get(topic) ?? [];
        lines.push(JSON.stringify(value) + "\n"); grouped.set(topic, lines);
      }
      const directory = path.join(dir, `2026/01/${String(day + 1).padStart(2, "0")}`);
      fs.mkdirSync(directory, { recursive: true });
      for (const [topic, lines] of grouped) {
        const text = lines.join(""); bytes += Buffer.byteLength(text); files++;
        fs.writeFileSync(path.join(directory, archiveFileName(topic)), text);
      }
    }
    fs.writeFileSync(path.join(root, "event-archive.json"), JSON.stringify({ version: 1, dir }));
    fs.writeFileSync(path.join(dir, "MESH.json"), JSON.stringify({ version: 1, firstSequence: 1, meshRoot: root }));
    fs.writeFileSync(path.join(root, "events.jsonl"), events.slice(-8).map(event => JSON.stringify(event) + "\n").join(""));
    fs.writeFileSync(path.join(root, "sequence"), String(sequence));
    return { root, dir, events, bytes, files, through: sequence };
  };
  // A real heartbeat measures responsiveness; it never supplies, mocks, delays or changes I/O.
  const responsive = async operation => {
    let finished = false, beats = 0, firstHeartbeatMs = null, maxHeartbeatGapMs = 0;
    const started = performance.now(), cpu = process.cpuUsage();
    let lastBeat = started;
    const beat = () => {
      if (finished) return;
      const now = performance.now(); beats++;
      firstHeartbeatMs ??= now - started;
      maxHeartbeatGapMs = Math.max(maxHeartbeatGapMs, now - lastBeat); lastBeat = now;
    };
    const immediate = setImmediate(beat), timer = setInterval(beat, 5);
    let value, yieldedBeforeCompletion;
    try { value = await operation(); yieldedBeforeCompletion = beats > 0; }
    finally { finished = true; clearImmediate(immediate); clearInterval(timer); }
    const wallMs = performance.now() - started, used = process.cpuUsage(cpu);
    return { value, metric: { wallMs, cpuMs: (used.user + used.system) / 1000, beats, firstHeartbeatMs,
      maxHeartbeatGapMs: Math.max(maxHeartbeatGapMs, performance.now() - lastBeat), yieldedBeforeCompletion } };
  };
  const f = makeFixture("archive-cold");
  const archiveCold = await responsive(() => new MeshArchive(f.dir, f.root).readAfterAsync(0, f.through, all, f.through + 1));
  same(archiveCold.value, f.events);
  assert(archiveCold.metric.yieldedBeforeCompletion, "Cold async archive scan blocked the heartbeat until completion");
  // Independent cold raw files: the archive probe must not pre-warm the real bridge path's index.
  const bridgeFixture = makeFixture("bridge-cold");
  const store = new MeshStore(bridgeFixture.root, 64 * 1024, 256, { stateBackend: "file" });
  const side = new StoreBridgeSide(store, "hub");
  let bridgeCold, bridgedIds;
  try {
    bridgeCold = await responsive(() => side.read(0));
    same(bridgeCold.value.events, bridgeFixture.events.slice(0, 256).filter(event => !event.data.bridge));
    assert(bridgeCold.metric.yieldedBeforeCompletion, "Cold StoreBridgeSide.read blocked the heartbeat until completion");
    bridgedIds = await responsive(() => side.bridgedIds(0));
    assert.deepEqual(bridgedIds.value, { ids: bridgeFixture.events.filter(event => event.data.bridge?.from === "hub").map(event => event.data.bridge.id),
      through: bridgeFixture.through, done: true });
    assert(bridgedIds.metric.yieldedBeforeCompletion, "Startup bridgedIds blocked the heartbeat until completion");
  } finally { store.closeState(); }
  const manifest = { bundle, dir: f.dir, root: f.root, through: f.through, after: f.through - 32, limit: 32 };
  const manifestFile = path.join(scratch, "manifest.json");
  fs.writeFileSync(manifestFile, JSON.stringify(manifest));
  const runs = [];
  for (let sample = 0; sample < samples; sample++) {
    const text = execFileSync(process.execPath, [script, "--worker", manifestFile, sample % 2 ? "candidate-first" : "legacy-first"],
      { encoding: "utf8", maxBuffer: 1024 * 1024, timeout: 60_000 });
    runs.push(JSON.parse(text));
  }
  const distribution = values => {
    const sorted = [...values].sort((left, right) => left - right);
    return { min: sorted[0], median: sorted[Math.floor(sorted.length / 2)], max: sorted.at(-1), samples: values };
  };
  const baseline = distribution(runs.map(run => run.baseline.wallMs));
  const indexed = distribution(runs.map(run => run.indexed.wallMs));
  const speedup = baseline.median / indexed.median;
  const workReduction = runs.map(run => ({ parse: run.work.baseline.parsedEvents / Math.max(1, run.work.indexed.parsedEvents),
    bytes: run.work.baseline.archiveBytesRead / Math.max(1, run.work.indexed.archiveBytesRead) }));
  const result = { host: os.hostname(), node: process.version, scratch, bundle, samples, profile,
    fixture: { events: f.through, files: f.files, archiveBytes: f.bytes, busyTopicBytes: fs.statSync(path.join(f.dir, "2026/01/01/fleet.work.large.jsonl")).size },
    query: { after: manifest.after, through: manifest.through, limit: manifest.limit },
    wallMs: { literalLegacy: baseline, reopenedIndexed: indexed, firstReopenedBeforeWarmup: distribution(runs.map(run => run.firstReopened.wallMs)) },
    cpuMs: { literalLegacy: distribution(runs.map(run => run.baseline.cpuMs)), reopenedIndexed: distribution(runs.map(run => run.indexed.cpuMs)) },
    speedup, requiredSpeedup: minSpeedup, timingPass: speedup > minSpeedup,
    work: { samples: runs.map(run => run.work), reduction: workReduction },
    responsiveness: { coldArchive: archiveCold.metric, coldStoreBridgeSideRead: bridgeCold.metric, startupBridgedIds: bridgedIds.metric },
    exact: true };
  const output = JSON.stringify(result, null, 2) + "\n";
  if (options["--out"]) fs.writeFileSync(path.resolve(options["--out"]), output);
  process.stdout.write(output);
  assert(workReduction.every(value => value.parse > 10 && value.bytes > 10), "Deterministic archive work reduction must exceed 10x");
  assert(result.timingPass, `Measured median speedup ${speedup.toFixed(2)}x does not exceed ${minSpeedup}x; report honestly, do not weaken the baseline`);
}
