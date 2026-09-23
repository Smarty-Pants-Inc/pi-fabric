// CLI for the shared prewalk comparison/archive helpers in
// bench/prewalk/lib/prewalk-bench-lib.mjs.
//
//   node bench/prewalk/compare-prewalk-runs.mjs <a.json[.gz]> <b.json[.gz]> [--out comparison.json] [--label-a name] [--label-b name]
//   node bench/prewalk/compare-prewalk-runs.mjs --archive <raw.json> [--gz raw.json.gz] [--remove-raw]
//
// Raw JSON and gzip archives are both accepted. Every output path is created
// exclusively: existing files are never overwritten. Without --out the
// comparison JSON is written to stdout.
import { pathToFileURL } from "node:url";
import { archiveRaw, buildComparison, readRunFile, writeExclusive } from "./lib/prewalk-bench-lib.mjs";

const USAGE = [
  "Usage:",
  "  compare-prewalk-runs.mjs <a.json|a.json.gz> <b.json|b.json.gz> [--out comparison.json] [--label-a name] [--label-b name]",
  "  compare-prewalk-runs.mjs --archive <raw.json> [--gz raw.json.gz] [--remove-raw]",
  "",
  "Comparison labels source/runner/settings/host compatibility and reports queue cells",
  "(requests, context bytes, per-worker spread) plus drift baseline-vs-clean stages.",
  "Archive gzips a raw run, verifies decompressed bytes, then optionally removes the raw file.",
].join("\n");

export function main(argv) {
  const options = { positional: [], out: null, labels: ["run-a", "run-b"], archive: null, gz: null, removeRaw: false };
  const valueOf = (index, flag) => {
    const value = argv[index];
    if (value === undefined) throw new Error(`${flag} needs a value`);
    return value;
  };
  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index];
    if (arg === "--help" || arg === "-h") {
      console.log(USAGE);
      return;
    }
    if (arg === "--out") { options.out = valueOf(index + 1, "--out"); index += 1; continue; }
    if (arg === "--label-a") { options.labels[0] = valueOf(index + 1, "--label-a"); index += 1; continue; }
    if (arg === "--label-b") { options.labels[1] = valueOf(index + 1, "--label-b"); index += 1; continue; }
    if (arg === "--archive") { options.archive = valueOf(index + 1, "--archive"); index += 1; continue; }
    if (arg === "--gz") { options.gz = valueOf(index + 1, "--gz"); index += 1; continue; }
    if (arg === "--remove-raw") { options.removeRaw = true; continue; }
    if (arg.startsWith("--")) throw new Error(`Unknown option: ${arg}`);
    options.positional.push(arg);
  }
  if (options.archive) {
    if (options.positional.length > 0) throw new Error("Pass the raw path with --archive; positional arguments are not accepted");
    const result = archiveRaw(options.archive, options.gz, { removeRaw: options.removeRaw });
    console.log(JSON.stringify({ ok: true, ...result }, null, 2));
    return;
  }
  if (options.positional.length !== 2) throw new Error(`Expected exactly two run paths, got ${options.positional.length}`);
  const inputs = options.positional.map((input) => readRunFile(input));
  const comparison = buildComparison(inputs, options.labels);
  const text = `${JSON.stringify(comparison, null, 2)}\n`;
  if (!options.out) {
    process.stdout.write(text);
    return;
  }
  const out = writeExclusive(options.out, text);
  console.log(JSON.stringify({
    ok: true,
    out,
    compatible: comparison.compatibility.comparable,
    sourceFingerprint: comparison.compatibility.checks.sourceFingerprint,
    queueCells: comparison.queue.cells.length,
    driftGroups: comparison.drift.groups.length,
    baselineVsCleanPairs: comparison.drift.cleanVsBaseline.length,
  }, null, 2));
}

const invokedDirectly = process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href;
if (invokedDirectly) {
  try {
    main(process.argv.slice(2));
  } catch (error) {
    console.error(`compare-prewalk-runs: ${error instanceof Error ? error.message : String(error)}`);
    process.exitCode = 1;
  }
}
