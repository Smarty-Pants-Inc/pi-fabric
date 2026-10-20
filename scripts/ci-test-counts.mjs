// Aggregate Vitest's machine-readable affected-test receipts, never scrape log text.
// Related and direct are separate phases: the old job intentionally ran overlaps twice.
import fs from "node:fs";
import path from "node:path";
import assert from "node:assert/strict";

const [root, os, countText] = process.argv.slice(2);
const count = Number(countText);
assert(root && ["ubuntu-latest", "windows-latest"].includes(os) && Number.isInteger(count) && count > 0,
  "usage: node scripts/ci-test-counts.mjs ROOT OS SHARD_COUNT");
const read = (file) => JSON.parse(fs.readFileSync(file, "utf8"));
const totals = new Map();
const filesByPhase = new Map();
let reference;
const lines = ["## Affected-test shard counts", "", "| Shard | Phase | Total | Passed | Failed | Skipped/pending | Todo |",
  "| --- | --- | ---: | ---: | ---: | ---: | ---: |"];
for (let index = 1; index <= count; index++) {
  const directory = path.join(root, `affected-${os}-${index}`);
  const { shard, platform, ...selection } = read(path.join(directory, "selection.json"));
  assert.equal(shard, `${index}/${count}`, "missing, duplicate, or misnumbered shard receipt");
  assert.equal(platform, os === "ubuntu-latest" ? "linux" : "win32");
  if (reference) assert.deepEqual(selection, reference, "shards must use the same affected selection");
  reference = selection;
  if (selection.phases.length === 0) lines.push(`| ${shard} | no selected tests | 0 | 0 | 0 | 0 | 0 |`);
  for (const phase of selection.phases) {
    assert(["related", "direct"].includes(phase));
    const report = read(path.join(directory, `${phase}.json`));
    const values = [report.numTotalTests, report.numPassedTests, report.numFailedTests,
      report.numPendingTests, report.numTodoTests];
    assert(values.every((value) => Number.isInteger(value) && value >= 0), "invalid Vitest counts");
    assert.equal(report.success, true, `shard ${shard} ${phase} did not pass`);
    const files = filesByPhase.get(phase) ?? new Set();
    for (const result of report.testResults) {
      const file = result.name.replaceAll("\\", "/").split("/tests/").at(-1);
      assert(!files.has(file), `${phase}: ${file} ran in more than one shard`);
      files.add(file);
    }
    filesByPhase.set(phase, files);
    lines.push(`| ${shard} | ${phase} | ${values.join(" | ")} |`);
    const sum = totals.get(phase) ?? [0, 0, 0, 0, 0];
    totals.set(phase, sum.map((value, i) => value + values[i]));
  }
}
for (const [phase, sum] of totals) lines.push(`| **SUM** | **${phase}** | ${sum.join(" | ")} |`);
lines.push("", "Counts include skipped/pending and todo cases. Related/direct overlaps remain separate, as before.",
  "Selection is ref-dependent: compare these sums with an unsharded hosted run of the same source/base, not another PR's totals.", "");
const output = lines.join("\n");
console.log(output);
if (process.env.GITHUB_STEP_SUMMARY) fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, output);
