// Aggregate Vitest's machine-readable affected-test receipts, never scrape log text.
// Related and direct are separate phases: the old job intentionally ran overlaps twice.
import fs from "node:fs";
import path from "node:path";
import assert from "node:assert/strict";
import { pathToFileURL } from "node:url";

// Pure: expected is the independently discovered, unsharded selection. A file
// may appear in both phases, but must occur in exactly one shard per phase.
export function compareFileParity(expected, shards) {
  return Object.fromEntries(["related", "direct"].map((phase) => {
    const wanted = new Set(expected[phase]);
    const occurrences = new Map();
    for (const shard of shards) {
      for (const file of new Set(shard[phase] ?? [])) {
        occurrences.set(file, (occurrences.get(file) ?? 0) + 1);
      }
    }
    const missing = [...wanted].filter((file) => !occurrences.has(file)).sort();
    const extra = [...occurrences.keys()].filter((file) => !wanted.has(file)).sort();
    const duplicated = [...occurrences].filter(([, count]) => count > 1).map(([file]) => file).sort();
    return [phase, { expected: wanted.size, actual: occurrences.size, missing, extra, duplicated,
      ok: missing.length === 0 && extra.length === 0 && duplicated.length === 0 }];
  }));
}

function main() {
  const [root, os, countText, selectionFile] = process.argv.slice(2);
  const count = Number(countText);
  assert(root && ["ubuntu-latest", "windows-latest"].includes(os) && Number.isInteger(count) && count > 0 && selectionFile,
    "usage: node scripts/ci-test-counts.mjs ROOT OS SHARD_COUNT SELECTION_JSON");
  const read = (file) => JSON.parse(fs.readFileSync(file, "utf8"));
  const totals = new Map();
  const filesByPhase = new Map();
  const shardFiles = [];
  const expected = read(selectionFile);
  for (const phase of ["related", "direct"]) {
    assert(Array.isArray(expected[phase]) && expected[phase].every((file) => typeof file === "string"),
      `invalid unsharded ${phase} selection`);
  }
  let reference;
  const lines = ["## Affected-test shard counts", "", "| Shard | Phase | Files | Total | Passed | Failed | Skipped | Todo | Test files",
    "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |"];
  const normalizedFile = (name) => `tests/${name.replaceAll("\\", "/").split("/tests/").at(-1)}`;
  const allFiles = new Map();
  for (let index = 1; index <= count; index++) {
    const directory = path.join(root, `affected-${os}-${index}`);
    const phaseFiles = { related: [], direct: [] };
    shardFiles.push(phaseFiles);
    const { shard, platform, ...selection } = read(path.join(directory, "selection.json"));
    assert.equal(shard, `${index}/${count}`, "missing, duplicate, or misnumbered shard receipt");
    assert.equal(platform, os === "ubuntu-latest" ? "linux" : "win32");
    if (reference) assert.deepEqual(selection, reference, "shards must use the same affected selection");
    reference = selection;
    if (selection.phases.length === 0) lines.push(`| ${shard} | no selected tests | 0 | 0 | 0 | 0 | 0 | 0 | — |`);
    for (const phase of selection.phases) {
      assert(["related", "direct"].includes(phase));
      const report = read(path.join(directory, `${phase}.json`));
      const values = [report.numTotalTests, report.numPassedTests, report.numFailedTests,
        report.numSkippedTests ?? Math.max(0, report.numPendingTests - report.numTodoTests), report.numTodoTests];
      assert(values.every((value) => Number.isInteger(value) && value >= 0), "invalid Vitest counts");
      assert.equal(report.success, true, `shard ${shard} ${phase} did not pass`);
      const files = [...new Set((report.testResults ?? []).map((result) => normalizedFile(result.name)))].sort();
      const seen = filesByPhase.get(phase) ?? new Set();
      for (const file of files) seen.add(file);
      phaseFiles[phase] = files;
      filesByPhase.set(phase, seen);
      allFiles.set(`${phase}:${shard}`, files);
      lines.push(`| ${shard} | ${phase} | ${files.length} | ${values.join(" | ")} | ${files.length ? files.join(", ") : "—"} |`);
      const sum = totals.get(phase) ?? [0, 0, 0, 0, 0];
      totals.set(phase, sum.map((value, i) => value + values[i]));
    }
  }
  for (const [phase, sum] of totals) lines.push(`| **SUM** | **${phase}** | **${[...filesByPhase.get(phase)].length}** | **${sum.join(" | ")}** | — |`);
  const linuxFiles = [...new Set([...allFiles].filter(([key]) => key.startsWith("direct:")).flatMap(([, files]) => files))].sort();
  lines.push("", `Linux actor-manager test file present: ${os === "ubuntu-latest" ? linuxFiles.includes("tests/actor-manager.test.ts") : "not applicable"}.`,
    "Counts include skipped and todo tests. Related/direct phases are separate.", "");
  const parity = compareFileParity(expected, shardFiles);
  lines.push("## File-level shard parity", "");
  for (const [phase, result] of Object.entries(parity)) {
    lines.push(`- **${phase}: ${result.ok ? "PASS" : "FAIL"}** — expected ${result.expected} files; shard union ${result.actual} files.`);
    if (!result.ok) {
      for (const kind of ["missing", "extra", "duplicated"]) {
        lines.push(`  - ${kind}: ${result[kind].length ? result[kind].join(", ") : "—"}`);
      }
    }
  }
  lines.push("");
  const output = lines.join("\n");
  console.log(output);
  if (process.env.GITHUB_STEP_SUMMARY) fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, output);
  if (Object.values(parity).some((result) => !result.ok)) process.exitCode = 1;
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) main();
