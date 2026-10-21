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
const lines = ["## Affected-test shard counts", "", "| Shard | Phase | Files | Total | Passed | Failed | Skipped | Todo | Test files",
  "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |"];
const normalizedFile = (name) => `tests/${name.replaceAll("\\", "/").split("/tests/").at(-1)}`;
const allFiles = new Map();
for (let index = 1; index <= count; index++) {
  const directory = path.join(root, `affected-${os}-${index}`);
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
    for (const file of files) {
      assert(!seen.has(file), `${phase}: ${file} ran in more than one shard`);
      seen.add(file);
    }
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
const output = lines.join("\n");
console.log(output);
if (process.env.GITHUB_STEP_SUMMARY) fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, output);
