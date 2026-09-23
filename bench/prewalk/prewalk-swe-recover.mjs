#!/usr/bin/env node
// Offline recovery and reclassification for frozen native SWE experiments.
//
// Reads a frozen experiment root (evidence/ + work/), reclassifies its control
// and candidate reports with the canonical classifiers in
// bench/prewalk/lib/prewalk-swe-evidence.mjs, optionally recovers one attempt whose
// patch capture failed (lock-safe, no model calls, no lock removal), regrades
// the recovered patch with the experiment's own frozen grader, and writes every
// derived artifact into a fresh --out directory. Original reports, results and
// checkpoints are never modified.
//
// usage: node bench/prewalk/prewalk-swe-recover.mjs --source <experiment-root> --out <fresh-dir> \
//        [--attempt <id>] [--grade-python <path>] [--grade-script <path>] [--grade-timeout-ms <ms>]
//
// --grade-python defaults to <source>/work/.venv/bin/python when present,
// otherwise "python3". The grader consumes the experiment's task manifest,
// private grader inputs and pinned images through SWE52_ROOT=<source> plus
// local Docker; grading containers run with network disabled.

import { execFile } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { promisify } from "node:util";
import { sha256, writeExclusive } from "./lib/prewalk-bench-lib.mjs";
import { classifyCandidate, classifyControls, comparePairs, summarizeAttempts } from "./lib/prewalk-swe-evidence.mjs";
import { capturePatch } from "./lib/prewalk-patch.mjs";

const execFileAsync = promisify(execFile);
const argv = process.argv.slice(2);
const value = (flag) => {
  const index = argv.indexOf(flag);
  return index >= 0 ? argv[index + 1] : undefined;
};
const usage = () => {
  console.error(
    "usage: node bench/prewalk/prewalk-swe-recover.mjs --source <experiment-root> --out <fresh-dir> " +
      "[--attempt <id>] [--grade-python <path>] [--grade-script <path>] [--grade-timeout-ms <ms>]",
  );
  process.exit(2);
};
const fail = (message) => {
  console.error(JSON.stringify({ ok: false, error: message }));
  process.exit(1);
};

const source = value("--source");
const out = value("--out");
const attemptId = value("--attempt");
const gradeTimeoutMs = Number(value("--grade-timeout-ms") ?? 900_000);
if (!source || !out) usage();
for (const candidate of [source, out]) {
  if (!path.isAbsolute(candidate)) fail("--source and --out must be absolute paths");
}
if (fs.existsSync(out)) fail(`--out must not exist: ${out}`);
for (const required of ["evidence/results.json", "evidence/task-manifest.json", "evidence/schedule.json"]) {
  if (!fs.existsSync(path.join(source, required))) fail(`missing ${required} under --source`);
}
const gradeScript = value("--grade-script") ?? path.join(source, "work/support/grade.py");
const defaultPython = path.join(source, "work/.venv/bin/python");
const gradePython = value("--grade-python") ?? (fs.existsSync(defaultPython) ? defaultPython : "python3");
const gradePythonIsPath = path.isAbsolute(gradePython) || gradePython.includes("/") || gradePython.includes("\\");
if (attemptId !== undefined && gradePythonIsPath && !fs.existsSync(gradePython)) fail(`--grade-python not found: ${gradePython}`);
if (!fs.existsSync(gradeScript)) fail(`grader script not found: ${gradeScript}`);
if (!Number.isInteger(gradeTimeoutMs) || gradeTimeoutMs <= 0) fail("--grade-timeout-ms must be a positive integer");

fs.mkdirSync(out, { recursive: true, mode: 0o700 });
fs.mkdirSync(path.join(out, "attempts"), { mode: 0o700 });
const E = (relative) => path.join(source, relative);
const consumed = {};
const hashFile = (file) => {
  if (!fs.existsSync(file)) fail(`required input missing: ${file}`);
  consumed[file] = sha256(fs.readFileSync(file));
  return JSON.parse(fs.readFileSync(file, "utf8"));
};
const readMaybe = (file) => (fs.existsSync(file) ? fs.readFileSync(file, "utf8") : "");
const pad3 = (n) => String(n).padStart(3, "0");

const rows = hashFile(E("evidence/results.json"));
hashFile(E("evidence/task-manifest.json"));
const schedule = hashFile(E("evidence/schedule.json"));

// ---- canonical control verdicts ------------------------------------------
const controlVerdicts = {};
for (const index of [...new Set(rows.map((row) => row.index))]) {
  const controlId = pad3(index + 1);
  if (controlVerdicts[controlId]) continue;
  const reportPath = E(`evidence/controls/${controlId}/report.json`);
  if (!fs.existsSync(reportPath)) {
    controlVerdicts[controlId] = { present: false, ok: false };
    continue;
  }
  const report = hashFile(reportPath);
  const arm = (kind) => {
    const entry = (report.results ?? []).find((result) => result.kind === kind);
    if (!entry) return null;
    return {
      result: {
        valid: entry.valid,
        resolved: entry.resolved,
        error: entry.error ?? null,
        parsedTests: entry.parsedTests ?? 0,
        requiredTests: entry.requiredTests ?? 0,
        requiredTestsPassed: entry.requiredTestsPassed ?? 0,
      },
      execution: {
        setupOk: entry.error == null && entry.exitCode != null,
        testStarted: entry.exitCode != null && entry.error == null,
        testExitCode: entry.exitCode ?? null,
        stdout: readMaybe(E(`evidence/controls/${controlId}/${kind}/workspace/stdout.log`)),
        stderr: readMaybe(E(`evidence/controls/${controlId}/${kind}/workspace/stderr.log`)),
      },
    };
  };
  controlVerdicts[controlId] = {
    present: true,
    instance_id: report.instance_id,
    ...classifyControls({ noop: arm("noop"), gold: arm("gold") }),
  };
}
const controlsPassedFor = (index) => controlVerdicts[pad3(index + 1)]?.ok === true;

// ---- attempt reclassification ---------------------------------------------
const evidenceDirFor = (row) =>
  row.result ? path.dirname(row.result) : row.sourceEvidence ?? E(`evidence/attempts/${row.id}`);
const classifyGradedAttempt = (row, gradeDir) => {
  const reportPath = path.join(gradeDir, "report.json");
  let gradeResult = null;
  let execution = null;
  if (fs.existsSync(reportPath)) {
    const report = hashFile(reportPath);
    gradeResult = (report.results ?? []).find((result) => result.kind === "solver") ?? null;
    execution = {
      setupOk: gradeResult?.error == null && gradeResult?.exitCode != null,
      testStarted: gradeResult?.exitCode != null && gradeResult?.error == null,
      testExitCode: gradeResult?.exitCode ?? null,
      stdout: readMaybe(path.join(gradeDir, "solver/workspace/stdout.log")),
      stderr: readMaybe(path.join(gradeDir, "solver/workspace/stderr.log")),
    };
  }
  const verdict = classifyCandidate({ result: gradeResult, controlsPassed: controlsPassedFor(row.index), execution });
  return { verdict, gradeReport: fs.existsSync(reportPath) ? reportPath : null };
};

const attempted = [];
const skipped = [];
for (const row of rows) {
  if (!row.modelRequests || row.modelRequests === 0) {
    skipped.push({
      id: row.id,
      index: row.index,
      mode: row.mode,
      reason: `${row.failureKind ?? "skipped"}: ${row.error ?? "no model call made"}`,
    });
    continue;
  }
  const dir = evidenceDirFor(row);
  const { verdict, gradeReport } = classifyGradedAttempt(row, path.join(dir, "grading"));
  attempted.push({
    id: row.id,
    index: row.index,
    mode: row.mode,
    usageUsd: row.usageUsd ?? 0,
    usageHoldUsd: row.usageHoldUsd ?? 0,
    timedOut: row.timedOut ?? false,
    solverDurationSeconds: row.solverDurationSeconds ?? null,
    baselineTree: row.baselineTree ?? null,
    resolved: verdict.status === "pass",
    validComparison: verdict.validComparison,
    verdict,
    evidence: dir,
    gradeReport,
  });
}

// ---- optional lock-safe recovery + offline regrade ------------------------
let recovered = null;
if (attemptId !== undefined) {
  const row = rows.find((candidate) => candidate.id === attemptId);
  if (!row) fail(`attempt ${attemptId} not found in results.json`);
  if (!row.modelRequests || row.modelRequests === 0) {
    fail(`attempt ${attemptId} made no model calls; there is nothing to recover`);
  }
  if (row.result && fs.existsSync(row.result)) {
    fail(`attempt ${attemptId} already has a graded result; refusing to regrade`);
  }
  const ready = hashFile(E(`evidence/attempts/${attemptId}/ready.json`));
  if (!ready.baseline) fail(`ready.json for ${attemptId} has no baseline commit`);
  const repo = E(`work/attempts/${attemptId}/repo`);
  if (!fs.existsSync(repo)) fail(`checkout for ${attemptId} missing: ${repo}`);
  const captureOut = path.join(out, "attempts", `${attemptId}-capture`);
  const captured = await capturePatch({ repo, baseline: ready.baseline, out: captureOut });
  const gradeOut = path.join(out, "attempts", attemptId, "grading");
  fs.mkdirSync(path.dirname(gradeOut), { recursive: true, mode: 0o700 });
  let gradeExit = 0;
  let gradeError = null;
  try {
    await execFileAsync(gradePython, [gradeScript, String(row.index), captured.patchPath, gradeOut], {
      env: { ...process.env, SWE52_ROOT: source, PYTHONDONTWRITEBYTECODE: "1" },
      timeout: gradeTimeoutMs,
      maxBuffer: 64 * 1024 * 1024,
    });
  } catch (error) {
    gradeExit = Number.isInteger(error?.code) ? error.code : 1;
    gradeError = String(error?.message ?? error);
  }
  const { verdict, gradeReport } = classifyGradedAttempt(row, gradeOut);
  recovered = {
    id: attemptId,
    index: row.index,
    mode: row.mode ?? "unknown",
    usageUsd: row.usageUsd ?? 0,
    usageHoldUsd: row.usageHoldUsd ?? 0,
    timedOut: row.timedOut ?? false,
    solverDurationSeconds: row.solverDurationSeconds ?? row.worker?.durationSeconds ?? null,
    baselineTree: row.baselineTree ?? ready.baselineTree ?? null,
    resolved: verdict.status === "pass",
    validComparison: verdict.validComparison,
    verdict,
    evidence: path.join(out, "attempts", attemptId),
    gradeReport,
    recovered: {
      patch: { path: captured.patchPath, sha256: captured.sha256, bytes: captured.bytes },
      gradeExit,
      gradeError,
      note: "offline recovery: no model calls, no lock removal, original checkout untouched",
    },
  };
  const position = attempted.findIndex((candidate) => candidate.id === attemptId);
  if (position >= 0) attempted[position] = recovered;
  else attempted.push(recovered);
}

// ---- derived outputs (fresh directory only) -------------------------------
const attemptedIds = new Set(attempted.map((attempt) => attempt.id));
const remaining = schedule
  .filter((entry) => !attemptedIds.has(entry.id))
  .map((entry) => {
    const skippedRow = skipped.find((candidate) => candidate.id === entry.id);
    const controls = controlVerdicts[pad3(entry.index + 1)] ?? { present: false, ok: false };
    return {
      id: entry.id,
      index: entry.index,
      mode: entry.mode,
      instance_id: entry.instance_id ?? null,
      previouslySkipped: skippedRow?.reason ?? null,
      controls: { present: controls.present ?? false, ok: controls.ok ?? false },
      eligibility: skippedRow ? (controls.ok ? "eligible-after-control-fix" : "blocked-controls") : "never-started",
    };
  });

const comparison = comparePairs(attempted);
const counts = summarizeAttempts([
  ...attempted.map((attempt) => ({ ...attempt, modelRequests: 1, validGrade: attempt.validComparison })),
  ...skipped.map((candidate) => ({ ...candidate, modelRequests: 0 })),
]);
const verdictCounts = {
  pass: attempted.filter((attempt) => attempt.verdict.status === "pass").length,
  fail: attempted.filter((attempt) => attempt.verdict.status === "fail").length,
  unobserved: attempted.filter((attempt) => attempt.verdict.status === "unobserved").length,
};

const record = (name, data) => writeExclusive(path.join(out, name), JSON.stringify(data, null, 2) + "\n");
record("inputs.json", { source, out, gradeScript, gradePython, consumed });
record("control-verdicts.json", controlVerdicts);
record("candidate-verdicts.json", attempted);
record("recovered-comparison.json", comparison);
record("remaining-work.json", remaining);
record(
  "recovered-summary.json",
  {
    at: new Date().toISOString(),
    source,
    out,
    processed: rows.length,
    attempted: attempted.length,
    skipped: skipped.length,
    counts,
    verdictCounts,
    recoveredAttempt: recovered ? recovered.id : null,
    budgetNote: "Recovery adds no model usage; see the experiment's own budget-status.json for the monitored total.",
  },
);

const lines = [];
lines.push("# Native SWE offline recovery receipt", "");
lines.push(`Generated: ${new Date().toISOString()}`);
lines.push(`Source: ${source}`);
lines.push(`Output: ${out}`, "");
lines.push("## Control verdicts", "");
lines.push("| control | noop | gold | ok |");
lines.push("|---|---|---|---|");
for (const [controlId, verdict] of Object.entries(controlVerdicts)) {
  lines.push(
    `| ${controlId} | ${verdict.present ? `${verdict.noop.status}/${verdict.noop.acceptable ? "acceptable" : "rejected"}` : "missing"} | ${verdict.present ? verdict.gold.status : "missing"} | ${verdict.ok} |`,
  );
}
lines.push("", "## Attempt verdicts", "");
lines.push("| id | mode | index | verdict | comparable | timedOut | usageUsd | holdUsd | reason |");
lines.push("|---|---|---|---|---|---|---|---|---|");
for (const attempt of attempted) {
  lines.push(
    `| ${attempt.id} | ${attempt.mode} | ${attempt.index} | ${attempt.verdict.status} | ${attempt.validComparison} | ${attempt.timedOut} | ${attempt.usageUsd} | ${attempt.usageHoldUsd} | ${attempt.verdict.reason} |`,
  );
}
if (recovered) {
  lines.push(
    "",
    `Recovered attempt ${recovered.id}: patch sha256 ${recovered.recovered.patch.sha256} (${recovered.recovered.patch.bytes} bytes), regrade exit ${recovered.recovered.gradeExit}, verdict ${recovered.verdict.status}.`,
  );
}
lines.push(
  "",
  "## Corrected comparison",
  "",
  `- Comparable pairs: ${comparison.comparablePairs}`,
  `- Both resolved: ${comparison.bothResolved}; Astra-only: ${comparison.astraOnlyResolved}; Prewalk-only: ${comparison.prewalkOnlyResolved}; both unresolved: ${comparison.bothUnresolved}`,
  `- Cost intervals (USD): Astra [${comparison.costIntervalUsd.astra.join(" .. ")}], Prewalk [${comparison.costIntervalUsd.prewalk.join(" .. ")}]`,
  `- Mean successful solver seconds: ${JSON.stringify(comparison.meanSuccessfulSolverSeconds)}`,
  "",
  "## Remaining work",
  "",
);
for (const entry of remaining) {
  lines.push(`- ${entry.id} (${entry.mode}, task ${entry.index}): ${entry.eligibility}${entry.previouslySkipped ? ` — was: ${entry.previouslySkipped}` : ""}`);
}
lines.push(
  "",
  "## Notes",
  "",
  "- No model calls were made during recovery; original artifacts are untouched (see inputs.json for consumed hashes).",
  "- The historical `completedPairs` counter counted skipped schedule entries as pairs; the corrected `comparablePairs` here supersedes it.",
  "- Automatic Main review, latency tuning and the remaining real-model coverage scenarios stay unresolved product decisions.",
);
writeExclusive(path.join(out, "report.md"), lines.join("\n") + "\n");

console.log(
  JSON.stringify({
    ok: true,
    out,
    counts,
    verdictCounts,
    comparablePairs: comparison.comparablePairs,
    recoveredAttempt: recovered ? recovered.id : null,
  }),
);
