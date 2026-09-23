// Stage/count diagnostic for the production prewalk filesystem-drift owner
// (src/prewalk/fs-drift.ts, bundled unmodified).
//
//   node bench/prewalk/probe-prewalk-drift.mjs --out docs/benchmarks/prewalk/2026-09-19/drift-probe.json
//   node bench/prewalk/probe-prewalk-drift.mjs --quick --scenarios mixed,ignored --files 4 --out /tmp/probe.json
//
// Instrumentation is injected at bundle time: esbuild resolves the module's
// node:child_process / node:fs/promises / node:crypto imports to counting
// probes. There are no runtime flags and no source edits. Each scenario builds
// a disposable fixture and runs three stages:
//   baseline  captureBaseline on the frozen tree (arm-time listing + stat manifest)
//   clean     evaluate with no filesystem change (no candidates, no hashing)
//   changed   append to the first listed file, then evaluate (one content hash)
// Counts are deterministic per scenario. Per instrumentation kind, wallMs is
// the first-to-last call span inside the stage while serviceMs sums individual
// call durations: statManifest/hash verification run 32 concurrent workers, so
// summed service can exceed wall; git subprocesses are sequential.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { performance } from "node:perf_hooks";
import { fileURLToPath, pathToFileURL } from "node:url";
import { build } from "esbuild";
import { round, roundStats, sha256, stats, writeExclusive } from "./lib/prewalk-bench-lib.mjs";

const self = fileURLToPath(import.meta.url);
const root = path.resolve(path.dirname(self), "../..");

// ---- bundle-time instrumentation ---- //

const STATE_SOURCE = String.raw`
const now = () => performance.now();
const state = {
  currentStage: null,
  stages: [],
  unscoped: [],
  beginStage(name) {
    if (state.currentStage) throw new Error("probe stage already open: " + state.currentStage.name);
    state.currentStage = { name, start: now(), ops: [] };
  },
  endStage() {
    const stage = state.currentStage;
    if (!stage) throw new Error("no probe stage open");
    state.currentStage = null;
    stage.wallMs = now() - stage.start;
    state.stages.push(stage);
    return stage;
  },
  begin(kind, label) {
    const op = { kind, label: label || "", start: now(), detail: {} };
    (state.currentStage ? state.currentStage.ops : state.unscoped).push(op);
    return op;
  },
  end(op, detail) {
    op.end = now();
    op.durationMs = op.end - op.start;
    if (detail) Object.assign(op.detail, detail);
    return op;
  },
  clear() {
    state.currentStage = null;
    state.stages = [];
    state.unscoped = [];
  },
  snapshot() {
    return { stages: state.stages, unscoped: state.unscoped.length };
  },
};
globalThis.__prewalkDriftProbe = state;
export default state;
`;

const CHILD_PROCESS_SOURCE = String.raw`
import { execFile as realExecFile } from "node:child_process";
import { promisify } from "node:util";
import probe from "probe:core";

const labelFor = (file, args) => {
  const list = Array.isArray(args) ? args : [];
  const sub = list[0] === "-C" ? list[2] : list[0];
  return [file, sub].filter(Boolean).join(" ");
};
const wrap = (file, args, options, callback) => {
  const op = probe.begin("subprocess", labelFor(file, args));
  const child = realExecFile(file, args, options, (error, stdout, stderr) => {
    const text = typeof stdout === "string" ? stdout : "";
    probe.end(op, {
      bytes: text.length,
      ...(text.includes("\u0000") ? { listedFiles: text.split("\u0000").filter(Boolean).length } : {}),
    });
    callback(error, stdout, stderr);
  });
  return child;
};
export const execFile = (file, args, options, callback) => wrap(file, args, options, callback);
execFile[promisify.custom] = (file, args, options) => new Promise((resolve, reject) => {
  wrap(file, args, options, (error, stdout, stderr) => (error ? reject(error) : resolve({ stdout, stderr })));
});
`;

const FS_SOURCE = String.raw`
import { readFile as realReadFile, readdir as realReaddir, stat as realStat } from "node:fs/promises";
import probe from "probe:core";

const wrap = (kind, fn) => async (...args) => {
  const op = probe.begin(kind, "");
  try {
    const result = await fn(...args);
    probe.end(op, kind === "readFile" ? { bytes: Buffer.byteLength(result) } : {});
    return result;
  } catch (error) {
    probe.end(op, { failed: true });
    throw error;
  }
};
export const readFile = wrap("readFile", realReadFile);
export const readdir = wrap("readdir", realReaddir);
export const stat = wrap("stat", realStat);
`;

const CRYPTO_SOURCE = String.raw`
import { createHash as realCreateHash } from "node:crypto";
import probe from "probe:core";

export const createHash = (algorithm) => {
  const op = probe.begin("hash", algorithm);
  const inner = realCreateHash(algorithm);
  let bytes = 0;
  return {
    update(data, encoding) {
      bytes += typeof data === "string" ? Buffer.byteLength(data, encoding) : data?.byteLength ?? 0;
      inner.update(data, encoding);
      return this;
    },
    digest(encoding) {
      probe.end(op, { bytes });
      return inner.digest(encoding);
    },
  };
};
`;

const PROBE_NAMESPACE = "prewalk-drift-probe";
const PROBE_SOURCES = { core: STATE_SOURCE, child_process: CHILD_PROCESS_SOURCE, fs: FS_SOURCE, crypto: CRYPTO_SOURCE };
const probePlugin = {
  name: "prewalk-drift-probe",
  setup(api) {
    api.onResolve({ filter: /^probe:core$/ }, () => ({ path: "core", namespace: PROBE_NAMESPACE }));
    const redirect = (filter, key) => api.onResolve({ filter }, (args) =>
      args.importer.endsWith(`${path.sep}fs-drift.ts`) ? { path: key, namespace: PROBE_NAMESPACE } : undefined);
    redirect(/^node:child_process$/, "child_process");
    redirect(/^node:fs\/promises$/, "fs");
    redirect(/^node:crypto$/, "crypto");
    api.onLoad({ filter: /.*/, namespace: PROBE_NAMESPACE }, (args) => {
      const contents = PROBE_SOURCES[args.path];
      if (!contents) throw new Error(`Unknown probe module: ${args.path}`);
      return { contents, loader: "js" };
    });
  },
};

// ---- fixtures ---- //

const SCENARIOS = [
  { id: "tracked", git: true, categories: { tracked: true } },
  { id: "already-dirty", git: true, categories: { dirty: true }, note: "committed, then modified before the baseline is captured" },
  { id: "staged", git: true, categories: { staged: true }, note: "git add, no commit" },
  { id: "untracked", git: true, categories: { untracked: true }, note: "matches the retained run1/run2 drift workload shape (git init + untracked files, no adds)" },
  { id: "ignored", git: true, categories: { ignored: true }, note: "one .gitignore entry; ignored files must not be listed or statted" },
  { id: "mixed", git: true, categories: { tracked: true, dirty: true, staged: true, untracked: true, ignored: true } },
  { id: "walk-untracked", git: false, categories: { untracked: true }, note: "no git: in-process walk listing instead of git ls-files" },
];

const CATEGORY_DIRS = { tracked: "tracked", dirty: "dirty", staged: "staged", untracked: "untracked", ignored: "ignored" };
const pad = (value) => String(value).padStart(4, "0");

const git = (cwd, args) => execFileSync("git", ["-C", cwd, "-c", "init.templateDir=", ...args], { stdio: "pipe" });
const commit = (cwd) => execFileSync("git", [
  "-C", cwd, "-c", "init.templateDir=", "-c", "user.email=probe@example.invalid", "-c", "user.name=prewalk-probe",
  "commit", "-qm", "probe base",
], { stdio: "pipe" });

export const expectedListedFiles = (scenario, perCategory) => Object.entries(scenario.categories)
  .reduce((total, [category, enabled]) => enabled ? total + (category === "ignored" ? 1 : perCategory) : total, 0);

function createFixture(scenario, perCategory, scratch) {
  const dir = fs.mkdtempSync(path.join(scratch, `${scenario.id}-`));
  if (scenario.git) git(dir, ["init", "-q"]);
  const files = { tracked: [], dirty: [], staged: [], untracked: [], ignored: [] };
  const write = (relative, content = "probe value\n") => {
    const absolute = path.join(dir, relative);
    fs.mkdirSync(path.dirname(absolute), { recursive: true });
    fs.writeFileSync(absolute, content);
    return relative;
  };
  const make = (category, prefix) => {
    if (!scenario.categories[category]) return;
    for (let index = 0; index < perCategory; index++) files[category].push(write(`${CATEGORY_DIRS[category]}/${prefix}${pad(index)}.txt`));
  };
  make("tracked", "t");
  make("dirty", "d");
  if (scenario.git && (files.tracked.length > 0 || files.dirty.length > 0)) {
    git(dir, ["add", "-A"]);
    commit(dir);
  }
  for (const relative of files.dirty) fs.appendFileSync(path.join(dir, relative), "dirty before baseline\n");
  make("staged", "s");
  if (scenario.git && files.staged.length > 0) git(dir, ["add", "--", CATEGORY_DIRS.staged]);
  make("untracked", "u");
  if (scenario.categories.ignored) {
    write(".gitignore", `${CATEGORY_DIRS.ignored}/\n`);
    make("ignored", "i");
  }
  const listed = [...files.tracked, ...files.dirty, ...files.staged, ...files.untracked];
  if (scenario.categories.ignored) listed.push(".gitignore");
  return { dir, listed, first: listed[0], ignoredFiles: files.ignored };
}

// ---- stage aggregation ---- //

function aggregateStage(stage) {
  const kinds = {};
  let totalServiceMs = 0;
  for (const op of stage.ops) {
    totalServiceMs += op.durationMs;
    if (!kinds[op.kind]) {
      kinds[op.kind] = { count: 0, serviceMs: 0, firstStart: op.start, lastEnd: op.end, bytes: 0, labels: {} };
    }
    const kind = kinds[op.kind];
    kind.count += 1;
    kind.serviceMs += op.durationMs;
    kind.firstStart = Math.min(kind.firstStart, op.start);
    kind.lastEnd = Math.max(kind.lastEnd, op.end);
    if (op.detail.bytes !== undefined) kind.bytes += op.detail.bytes;
    if (!kind.labels[op.label]) kind.labels[op.label] = { count: 0, serviceMs: 0 };
    kind.labels[op.label].count += 1;
    kind.labels[op.label].serviceMs += op.durationMs;
    if (op.detail.listedFiles !== undefined) {
      kind.labels[op.label].listedFiles = (kind.labels[op.label].listedFiles ?? 0) + op.detail.listedFiles;
    }
  }
  for (const kind of Object.values(kinds)) {
    kind.wallMs = round(kind.lastEnd - kind.firstStart);
    kind.serviceMs = round(kind.serviceMs);
    delete kind.firstStart;
    delete kind.lastEnd;
    for (const label of Object.values(kind.labels)) label.serviceMs = round(label.serviceMs);
    if (kind.bytes === 0) delete kind.bytes;
    else kind.bytes = Math.round(kind.bytes);
  }
  return { wallMs: round(stage.wallMs), totalServiceMs: round(totalServiceMs), kinds };
}

export function summarizeStage(runs) {
  const kindNames = [...new Set(runs.flatMap((run) => Object.keys(run.kinds)))].sort();
  const kinds = {};
  for (const name of kindNames) {
    // kindNames is the union across runs; a run may lack a kind the other
    // observed (a git-listing fallback changes the stat/subprocess mix), so
    // aggregate only the samples that actually recorded it.
    const entries = runs.map((run) => run.kinds[name]).filter(Boolean);
    const labels = {};
    for (const labelName of [...new Set(entries.flatMap((entry) => Object.keys(entry.labels)))].sort()) {
      const labelEntries = entries.map((entry) => entry.labels[labelName]).filter(Boolean);
      labels[labelName] = {
        count: labelEntries[0].count,
        serviceMs: roundStats(stats(labelEntries.map((entry) => entry.serviceMs))),
        ...(labelEntries[0].listedFiles !== undefined ? { listedFiles: labelEntries[0].listedFiles } : {}),
      };
    }
    kinds[name] = {
      count: entries[0].count,
      serviceMs: roundStats(stats(entries.map((entry) => entry.serviceMs))),
      wallMs: roundStats(stats(entries.map((entry) => entry.wallMs))),
      ...(entries[0].bytes !== undefined ? { bytes: roundStats(stats(entries.map((entry) => entry.bytes))) } : {}),
      labels,
    };
  }
  return {
    samples: runs.length,
    wallMs: roundStats(stats(runs.map((run) => run.wallMs))),
    serviceMs: roundStats(stats(runs.map((run) => run.totalServiceMs))),
    kinds,
  };
}

async function runScenario(scenario, options, scratch, PrewalkDriftTracker) {
  const probe = globalThis.__prewalkDriftProbe;
  const stageRuns = { baseline: [], clean: [], changed: [] };
  const failures = [];
  const expected = expectedListedFiles(scenario, options.files);
  for (let iteration = 0; iteration < options.iterations; iteration++) {
    const fixture = createFixture(scenario, options.files, scratch);
    try {
      const tracker = new PrewalkDriftTracker();
      probe.clear();
      probe.beginStage("baseline");
      await tracker.captureBaseline("probe", fixture.dir);
      const baseline = aggregateStage(probe.endStage());
      probe.beginStage("clean");
      const cleanClaim = await tracker.evaluate("probe", fixture.dir);
      const clean = aggregateStage(probe.endStage());
      probe.beginStage("changed");
      fs.appendFileSync(path.join(fixture.dir, fixture.first), `\nchange ${iteration}\n`);
      const changedClaim = await tracker.evaluate("probe", fixture.dir);
      const changed = aggregateStage(probe.endStage());
      if (probe.snapshot().unscoped !== 0) failures.push("instrumented operations outside a stage");
      const listed = scenario.git ? baseline.kinds.subprocess?.labels?.["git ls-files"]?.listedFiles ?? 0 : expected;
      const statCount = baseline.kinds.stat?.count ?? 0;
      const baselineHashes = baseline.kinds.hash?.count ?? 0;
      const cleanHashes = clean.kinds.hash?.count ?? 0;
      const changedHashes = changed.kinds.hash?.count ?? 0;
      if (listed !== expected) failures.push(`baseline listedFiles=${listed}, expected ${expected}`);
      if (statCount !== expected) failures.push(`baseline stat count=${statCount}, expected ${expected}`);
      if (scenario.git && !baseline.kinds.subprocess) failures.push("git scenario had no listing subprocess");
      if (!scenario.git) {
        const discovery = baseline.kinds.subprocess?.count ?? 0;
        if (discovery !== 1) failures.push(`walk scenario expected one failed git discovery probe, saw ${discovery} subprocesses`);
        else if (!baseline.kinds.subprocess.labels?.["git rev-parse"]) failures.push("walk scenario's subprocess was not the git discovery probe");
        if (!baseline.kinds.readdir) failures.push("walk scenario had no readdir listing");
      }
      if (baselineHashes !== 0) failures.push(`baseline hashed ${baselineHashes} files, expected 0`);
      if (cleanClaim !== undefined) failures.push("clean evaluation claimed drift");
      if (cleanHashes !== 0) failures.push(`clean hashed ${cleanHashes} files, expected 0`);
      if (!changedClaim) failures.push("changed evaluation did not claim");
      if (changedHashes !== 1) failures.push(`changed hashed ${changedHashes} files, expected 1`);
      if (changedClaim && !changedClaim.files.includes(fixture.first)) failures.push(`changed claim ${JSON.stringify(changedClaim.files)} missing ${fixture.first}`);
      stageRuns.baseline.push(baseline);
      stageRuns.clean.push(clean);
      stageRuns.changed.push(changed);
    } finally {
      fs.rmSync(fixture.dir, { recursive: true, force: true});
    }
  }
  return {
    id: scenario.id,
    git: scenario.git,
    categories: Object.keys(scenario.categories),
    note: scenario.note ?? null,
    expectedListedFiles: expected,
    checks: { failures: [...new Set(failures)], passed: failures.length === 0 },
    stages: {
      baseline: summarizeStage(stageRuns.baseline),
      clean: summarizeStage(stageRuns.clean),
      changed: summarizeStage(stageRuns.changed),
    },
  };
}

// ---- CLI ---- //

const USAGE = [
  "Usage: probe-prewalk-drift.mjs --out <path> [--quick] [--files N] [--iterations N] [--scenarios a,b]",
  "",
  "--out         required; refuses to overwrite an existing file",
  "--quick       one iteration per scenario",
  "--files       files per category per fixture (default 40)",
  "--scenarios   comma-separated subset of: " + SCENARIOS.map((scenario) => scenario.id).join(","),
].join("\n");

export async function main(argv) {
  const options = { out: null, quick: false, files: 40, iterations: 3, scenarios: null };
  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index];
    if (arg === "--help" || arg === "-h") { console.log(USAGE); return; }
    if (arg === "--out") { options.out = argv[index + 1]; index += 1; continue; }
    if (arg === "--quick") { options.quick = true; continue; }
    if (arg === "--files") { options.files = Number(argv[index + 1]); index += 1; continue; }
    if (arg === "--iterations") { options.iterations = Number(argv[index + 1]); index += 1; continue; }
    if (arg === "--scenarios") { options.scenarios = (argv[index + 1] ?? "").split(",").map((value) => value.trim()).filter(Boolean); index += 1; continue; }
    throw new Error(`Unknown argument: ${arg}`);
  }
  if (!options.out) throw new Error("Specify --out <path>");
  if (!Number.isInteger(options.files) || options.files <= 0) throw new Error("--files must be a positive integer");
  if (options.quick) options.iterations = 1;
  if (!Number.isInteger(options.iterations) || options.iterations <= 0) throw new Error("--iterations must be a positive integer");
  const selected = SCENARIOS.filter((scenario) => !options.scenarios || options.scenarios.includes(scenario.id));
  if (selected.length === 0) throw new Error(`No scenario matched ${JSON.stringify(options.scenarios)}`);

  const startedAt = new Date().toISOString();
  const started = performance.now();
  // The bundled module keeps its @earendil-works imports external, so it must
  // resolve node_modules from inside the project; fixtures stay in tmp.
  const bundleScratch = fs.mkdtempSync(path.join(root, ".probe-prewalk-drift-"));
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "prewalk-drift-probe-"));
  try {
    const bundlePath = path.join(bundleScratch, "fs-drift.probe.mjs");
    const bundle = await build({
      entryPoints: [path.join(root, "src/prewalk/fs-drift.ts")],
      outfile: bundlePath,
      bundle: true,
      packages: "external",
      platform: "node",
      format: "esm",
      plugins: [probePlugin],
      metafile: true,
      logLevel: "silent",
    });
    const { PrewalkDriftTracker } = await import(pathToFileURL(bundlePath).href);
    const scenarios = [];
    for (const scenario of selected) scenarios.push(await runScenario(scenario, options, scratch, PrewalkDriftTracker));
    const failures = scenarios.flatMap((scenario) => scenario.checks.failures.map((failure) => `${scenario.id}: ${failure}`));
    const result = {
      schemaVersion: 1,
      tool: "probe-prewalk-drift",
      status: failures.length === 0 ? "completed" : "checks_failed",
      startedAt,
      finishedAt: new Date().toISOString(),
      elapsedMs: round(performance.now() - started),
      units: { time: "milliseconds", bytes: "bytes", counts: "operations" },
      provenance: {
        source: "src/prewalk/fs-drift.ts (production owner, unmodified)",
        sourceSha256: sha256(fs.readFileSync(path.join(root, "src/prewalk/fs-drift.ts"))),
        runnerSha256: sha256(fs.readFileSync(self)),
        bundleSha256: sha256(fs.readFileSync(bundlePath)),
        bundleInputs: Object.keys(bundle.metafile.inputs).filter((input) => input !== "<stdin>").sort(),
        head: execFileSync("git", ["-C", root, "rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
      },
      host: {
        node: process.version,
        platform: process.platform,
        arch: process.arch,
        cpus: os.cpus().length,
        cpu: os.cpus()[0]?.model,
        loadavg: os.loadavg(),
        hostPackages: {
          "pi-coding-agent": JSON.parse(fs.readFileSync(path.join(root, "node_modules/@earendil-works/pi-coding-agent/package.json"), "utf8")).version,
        },
      },
      method: {
        filesPerCategory: options.files,
        iterations: options.iterations,
        instrumentation: "bundle-time esbuild redirects of node:child_process / node:fs/promises / node:crypto; no runtime flags or source edits",
        wallVsService: "per kind, wallMs is the first-to-last instrumented call span in the stage; serviceMs sums individual call durations; concurrent stat/hash workers can make service exceed wall; git subprocesses are sequential",
        stages: {
          baseline: "captureBaseline on the frozen fixture (listing + stat manifest; no content hashing)",
          clean: "evaluate with no filesystem change (listing + stat; no candidates, no hashing)",
          changed: "append to the first listed file, then evaluate (one candidate, one content hash)",
        },
      },
      scenarios,
      checks: { allPassed: failures.length === 0, failures },
    };
    const out = writeExclusive(options.out, `${JSON.stringify(result, null, 2)}\n`);
    console.log(JSON.stringify({
      ok: failures.length === 0,
      out,
      scenarios: scenarios.map((scenario) => ({ id: scenario.id, expectedListedFiles: scenario.expectedListedFiles, failures: scenario.checks.failures })),
      elapsedMs: result.elapsedMs,
    }, null, 2));
    if (failures.length > 0) process.exitCode = 1;
  } finally {
    fs.rmSync(bundleScratch, { recursive: true, force: true });
    fs.rmSync(scratch, { recursive: true, force: true });
  }
}

const invokedDirectly = process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href;
if (invokedDirectly) {
  main(process.argv.slice(2)).catch((error) => {
    console.error(`probe-prewalk-drift: ${error instanceof Error ? error.message : String(error)}`);
    process.exitCode = 1;
  });
}
