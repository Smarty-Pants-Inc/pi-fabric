// Shared evidence logic for the prewalk benchmark tooling: aggregation,
// comparison and archive helpers used by bench/prewalk/benchmark-prewalk.mjs,
// bench/prewalk/compare-prewalk-runs.mjs and bench/prewalk/probe-prewalk-drift.mjs.
//
// Units are explicit at this boundary: every time value is milliseconds and
// every size value is bytes. stats() is nearest-rank, the same percentile
// definition the archived run1/run2 summaries used, so regenerated numbers
// stay comparable with the stored evidence.
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";

export const sha256 = (value) => createHash("sha256").update(value).digest("hex");

export const round = (value, digits = 4) => Number(value.toFixed(digits));

export function stats(values) {
  if (!Array.isArray(values) || values.length === 0) throw new Error("stats() needs at least one value");
  if (!values.every((value) => Number.isFinite(value))) throw new Error("stats() needs finite numbers");
  const sorted = [...values].sort((a, b) => a - b);
  const q = (p) => sorted[Math.max(0, Math.ceil(p * sorted.length) - 1)];
  return { n: sorted.length, min: sorted[0], median: q(0.5), p95: q(0.95), max: sorted.at(-1) };
}

export const roundStats = (value, digits = 4) => {
  if (value === null || value === undefined) return null;
  return {
    n: value.n,
    min: round(value.min, digits),
    median: round(value.median, digits),
    p95: round(value.p95, digits),
    max: round(value.max, digits),
  };
};

export function groupRows(rows, key, summarize) {
  const groups = new Map();
  for (const row of rows) {
    const id = key(row);
    if (!groups.has(id)) groups.set(id, []);
    groups.get(id).push(row);
  }
  return [...groups].map(([id, values]) => ({ id, ...summarize(values) }));
}

export function writeExclusive(outPath, contents) {
  const absolute = path.resolve(outPath);
  if (fs.existsSync(absolute)) throw new Error(`Refusing to overwrite existing file: ${absolute}`);
  fs.writeFileSync(absolute, contents, { flag: "wx" });
  return absolute;
}

// Read a benchmark run from raw JSON or a gzip archive. The reported SHA-256
// is always over the decompressed raw bytes, so it identifies the run itself;
// the gzip SHA-256 is reported separately when the input was compressed.
export function readRunFile(inputPath) {
  const absolute = path.resolve(inputPath);
  const fileBytes = fs.readFileSync(absolute);
  const gzipped = absolute.endsWith(".gz");
  const rawBytes = gzipped ? zlib.gunzipSync(fileBytes) : fileBytes;
  let run;
  try {
    run = JSON.parse(rawBytes.toString("utf8"));
  } catch (error) {
    throw new Error(`${absolute}: not valid JSON after ${gzipped ? "gunzip" : "read"}: ${error instanceof Error ? error.message : String(error)}`);
  }
  return {
    path: absolute,
    gzipped,
    rawBytes: rawBytes.length,
    rawSha256: sha256(rawBytes),
    gzSha256: gzipped ? sha256(fileBytes) : null,
    run,
  };
}

// ---- comparison ----

const presence = (value) => value !== undefined && value !== null;

const compareValues = (a, b) => {
  if (!presence(a) && !presence(b)) return "missing";
  if (!presence(a) || !presence(b)) return "differ";
  return isDeepStrictEqual(a, b) ? "match" : "differ";
};

export function compatibilityReport(inputs) {
  const runs = inputs.map((input) => input.run);
  const first = runs[0] ?? {};
  const provenance = (run) => run.provenance ?? {};
  const environment = (run) => run.environment ?? {};
  const checks = {
    schemaVersion: compareValues(first.schemaVersion, (runs[1] ?? {}).schemaVersion),
    sourceFingerprint: compareValues(provenance(first).sourceFingerprint, provenance(runs[1] ?? {}).sourceFingerprint),
    sourceHashes: compareValues(provenance(first).sourceHashes, provenance(runs[1] ?? {}).sourceHashes),
    runnerSha256: compareValues(provenance(first).runnerSha256, provenance(runs[1] ?? {}).runnerSha256),
    settings: compareValues(first.settings, (runs[1] ?? {}).settings),
    node: compareValues(environment(first).node, environment(runs[1] ?? {}).node),
    platform: compareValues(environment(first).platform, environment(runs[1] ?? {}).platform),
    arch: compareValues(environment(first).arch, environment(runs[1] ?? {}).arch),
    cpus: compareValues(environment(first).cpus, environment(runs[1] ?? {}).cpus),
    hostPackages: compareValues(environment(first).hostPackages, environment(runs[1] ?? {}).hostPackages),
  };
  const comparableKeys = ["schemaVersion", "sourceFingerprint", "runnerSha256", "settings"];
  const hostKeys = ["node", "platform", "arch", "cpus", "hostPackages"];
  return {
    checks,
    comparable: comparableKeys.every((key) => checks[key] === "match"),
    hostComparable: hostKeys.every((key) => checks[key] === "match"),
    notes: [
      "comparable requires schemaVersion, sourceFingerprint, runnerSha256 and settings to match",
      "host labels are advisory: differing host load invalidates absolute-time deltas but not shapes",
      "sourceHashes is a stronger per-file label; the fingerprint is the gate",
    ],
  };
}

const queueId = (row) => [
  row.prewalk ? "prewalk" : "control",
  row.followUpMode,
  `steers=${row.steers}`,
  `history=${row.history}`,
  row.profile,
].join("/");

const collectQueue = (run) => (run.workers ?? []).flatMap((worker) =>
  (worker.queue ?? []).map((row) => ({ ...row, workerId: worker.workerId })));

const summarizeRows = (rows, pick) => {
  if (!rows || rows.length === 0) return null;
  return roundStats(stats(rows.map(pick)));
};

const perWorkerSpread = (rows, pick) => {
  if (!rows || rows.length === 0) return null;
  const byWorker = new Map();
  for (const row of rows) {
    const values = byWorker.get(row.workerId) ?? [];
    values.push(pick(row));
    byWorker.set(row.workerId, values);
  }
  const medians = [...byWorker.values()].map((values) => stats(values).median);
  return {
    workers: medians.length,
    medianOfWorkerMediansMs: round(stats(medians).median),
    spreadMs: round(stats(medians).max - stats(medians).min),
  };
};

export function compareQueue(inputs, labels) {
  const cells = new Map();
  inputs.forEach((input, index) => {
    const label = labels[index];
    for (const row of collectQueue(input.run)) {
      const id = queueId(row);
      if (!cells.has(id)) {
        cells.set(id, {
          id,
          config: Object.fromEntries(["prewalk", "followUpMode", "steers", "history", "profile"].map((key) => [key, row[key]])),
          rows: {},
        });
      }
      const cell = cells.get(id);
      (cell.rows[label] ??= []).push(row);
    }
  });
  return [...cells.values()].map((cell) => {
    const values = (pick) => Object.fromEntries(labels.map((label) => [label, pick(label)]));
    const medians = labels.map((label) => summarizeRows(cell.rows[label], (row) => row.totalMs));
    const delta = medians[0] && medians[1] ? round(medians[1].median - medians[0].median) : null;
    return {
      id: cell.id,
      config: cell.config,
      samples: values((label) => cell.rows[label]?.length ?? 0),
      totalMs: values((label) => summarizeRows(cell.rows[label], (row) => row.totalMs)),
      perWorker: values((label) => perWorkerSpread(cell.rows[label], (row) => row.totalMs)),
      requestsMedian: values((label) => summarizeRows(cell.rows[label], (row) => row.requests?.length ?? 0)),
      contextBytesMedian: values((label) => summarizeRows(cell.rows[label], (row) => row.totalContextBytes ?? 0)),
      deltaMedianTotalMs: delta,
    };
  }).sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
}

const collectDrift = (run) => (run.workers ?? []).flatMap((worker) =>
  (worker.drift?.samples ?? []).map((sample) => ({ ...sample, workerId: worker.workerId })));

export function compareDrift(inputs, labels) {
  const groups = new Map();
  inputs.forEach((input, index) => {
    const label = labels[index];
    for (const sample of collectDrift(input.run)) {
      const id = `${sample.git ? "git" : "walk"}/${sample.count}/${sample.name}`;
      if (!groups.has(id)) groups.set(id, { id, rows: {} });
      const group = groups.get(id);
      (group.rows[label] ??= []).push(sample);
    }
  });
  return [...groups.values()].map((group) => ({
    id: group.id,
    samples: Object.fromEntries(labels.map((label) => [label, group.rows[label]?.length ?? 0])),
    wallMs: Object.fromEntries(labels.map((label) => [label, summarizeRows(group.rows[label], (row) => row.wallMs)])),
    cpuMs: Object.fromEntries(labels.map((label) => [label, summarizeRows(group.rows[label], (row) => row.cpuMs)])),
    claims: Object.fromEntries(labels.map((label) => [label, (group.rows[label] ?? []).filter((row) => row.reported !== null).length])),
  })).sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
}

// Pair each fixture's arm-time baseline capture with its unchanged-tree clean
// evaluation. Deltas are descriptive only: they move with host load.
export function compareBaselineVsClean(groups, labels) {
  const byId = new Map(groups.map((group) => [group.id, group]));
  const pairs = [];
  for (const group of groups) {
    if (!group.id.endsWith("/baseline")) continue;
    const prefix = group.id.slice(0, -"/baseline".length);
    const clean = byId.get(`${prefix}/clean`);
    if (!clean) continue;
    pairs.push({
      id: prefix,
      baselineWallMs: group.wallMs,
      cleanWallMs: clean.wallMs,
      deltaCleanMinusBaselineMs: Object.fromEntries(labels.map((label) => {
        const before = group.wallMs[label]?.median;
        const after = clean.wallMs[label]?.median;
        return [label, before !== undefined && after !== undefined ? round(after - before) : null];
      })),
    });
  }
  return pairs;
}

export function buildComparison(inputs, labels = ["run-a", "run-b"]) {
  if (inputs.length !== 2) throw new Error(`Comparison needs exactly two runs, got ${inputs.length}`);
  const driftGroups = compareDrift(inputs, labels);
  return {
    schemaVersion: 1,
    tool: "compare-prewalk-runs",
    generatedAt: new Date().toISOString(),
    labels,
    units: {
      time: "milliseconds (all *_ms keys)",
      bytes: "bytes (all *_bytes keys)",
      context: "totalContextBytes serializes Pi model-input message objects, not provider wire bytes or tokens",
    },
    runs: inputs.map((input, index) => ({
      label: labels[index],
      path: input.path,
      rawSha256: input.rawSha256,
      gzSha256: input.gzSha256,
      rawBytes: input.rawBytes,
      status: input.run.status ?? null,
      schemaVersion: input.run.schemaVersion ?? null,
      startedAt: input.run.startedAt ?? null,
      finishedAt: input.run.finishedAt ?? null,
      elapsedMs: input.run.elapsedMs ?? null,
      environment: input.run.environment ?? null,
      settings: input.run.settings ?? null,
      provenance: input.run.provenance ?? null,
      findings: input.run.summary?.findings ?? [],
    })),
    compatibility: compatibilityReport(inputs),
    queue: { cells: compareQueue(inputs, labels) },
    drift: { groups: driftGroups, cleanVsBaseline: compareBaselineVsClean(driftGroups, labels) },
    caveats: [
      "Absolute times are host-load sensitive; deltas are descriptive, not optimization claims.",
      "contextBytes is serialized Pi model-input message bytes, not provider wire bytes or tokens.",
      "A sourceFingerprint mismatch means the runs measured different source; do not read deltas as A/B results.",
    ],
  };
}

// Gzip a raw run, verify the decompressed bytes equal the raw input, and only
// then (optionally) remove the raw file. The archive is written with an
// exclusive-create flag and every existing output is refused.
export function archiveRaw(rawPath, gzPath, options = {}) {
  const absoluteRaw = path.resolve(rawPath);
  const absoluteGz = path.resolve(gzPath ?? `${absoluteRaw}.gz`);
  if (absoluteRaw === absoluteGz) throw new Error("Raw and archive paths must differ");
  if (absoluteRaw.endsWith(".gz")) throw new Error(`--archive expects a raw JSON run, not a gzip archive: ${absoluteRaw}`);
  if (!fs.existsSync(absoluteRaw)) throw new Error(`Raw run not found: ${absoluteRaw}`);
  if (fs.existsSync(absoluteGz)) throw new Error(`Refusing to overwrite existing archive: ${absoluteGz}`);
  const raw = fs.readFileSync(absoluteRaw);
  const rawSha256 = sha256(raw);
  const gz = zlib.gzipSync(raw, { level: 9 });
  const gzSha256 = sha256(gz);
  fs.writeFileSync(absoluteGz, gz, { flag: "wx" });
  let verified;
  try {
    verified = Buffer.compare(zlib.gunzipSync(fs.readFileSync(absoluteGz)), raw) === 0;
  } catch {
    verified = false;
  }
  if (!verified) {
    fs.rmSync(absoluteGz, { force: true });
    throw new Error(`Archive verification failed (decompressed bytes differ); removed ${absoluteGz}`);
  }
  const removedRaw = options.removeRaw === true;
  if (removedRaw) fs.rmSync(absoluteRaw);
  return {
    rawPath: absoluteRaw,
    gzPath: absoluteGz,
    rawSha256,
    gzSha256,
    rawBytes: raw.length,
    gzBytes: gz.length,
    verified,
    removedRaw,
    checksums: [`${gzSha256}  ${absoluteGz}`, `${rawSha256}  ${absoluteRaw}`],
  };
}

// ---- canonical tree snapshots ----

// A snapshot value may be a bare digest (older scratch verifiers wrote those)
// or an object written by snapshotTree(). Canonicalizing both forms is what
// makes an object-versus-string comparison impossible to get wrong.
const canonicalSnapshotValue = (value, entry) => {
  if (typeof value === "string") {
    if (!/^[0-9a-f]{64}$/.test(value)) throw new Error(`Invalid snapshot digest for ${entry}`);
    return { kind: "file", digest: value };
  }
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    const keys = Object.keys(value);
    if (keys.length === 1 && typeof value.sha256 === "string") {
      if (!/^[0-9a-f]{64}$/.test(value.sha256)) throw new Error(`Invalid snapshot digest for ${entry}`);
      return { kind: "file", digest: value.sha256 };
    }
    if (keys.length === 1 && typeof value.link === "string") {
      return { kind: "link", target: value.link };
    }
  }
  throw new Error(`Unsupported snapshot entry for ${entry}: ${JSON.stringify(value)}`);
};

// Walk a directory into a relative-POSIX-path map of { sha256 } or { link }.
// Paths are relative to options.relativeTo (default: the walked root), so two
// snapshots taken from different roots stay comparable.
export function snapshotTree(root, options = {}) {
  const absolute = path.resolve(root);
  const relativeTo = path.resolve(options.relativeTo ?? absolute);
  const skip = (options.skip ?? []).map((entry) => String(entry).replace(/\/+$/, ""));
  const result = {};
  if (!fs.existsSync(absolute)) return result;
  const visit = (dir) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      const rel = path.relative(relativeTo, full).split(path.sep).join("/");
      if (skip.some((skipped) => rel === skipped || rel.startsWith(`${skipped}/`))) continue;
      if (entry.isSymbolicLink()) result[rel] = { link: fs.readlinkSync(full) };
      else if (entry.isDirectory()) visit(full);
      else if (entry.isFile()) result[rel] = { sha256: sha256(fs.readFileSync(full)) };
    }
  };
  visit(absolute);
  return result;
}

// Compare two snapshots after canonicalizing both value shapes. Returns the
// exact changed/added/removed path lists; same is true only when all three are
// empty. A shape difference with an equal digest is not a change; a digest
// difference, a link-target difference, or a file becoming a link is.
export function compareSnapshots(before, after) {
  const beforeMap = before ?? {};
  const afterMap = after ?? {};
  const changed = [], added = [], removed = [];
  for (const [entry, value] of Object.entries(beforeMap)) {
    if (!(entry in afterMap)) { removed.push(entry); continue; }
    const a = canonicalSnapshotValue(value, entry);
    const b = canonicalSnapshotValue(afterMap[entry], entry);
    const equal = a.kind === b.kind && (a.kind === "file" ? a.digest === b.digest : a.target === b.target);
    if (!equal) changed.push(entry);
  }
  for (const entry of Object.keys(afterMap)) if (!(entry in beforeMap)) added.push(entry);
  changed.sort(); added.sort(); removed.sort();
  return { same: changed.length === 0 && added.length === 0 && removed.length === 0, changed, added, removed };
}
