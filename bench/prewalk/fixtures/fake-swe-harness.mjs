#!/usr/bin/env node
// Deterministic fake SWE harness for prewalk-swe-run coordinator tests.
// Mirrors the subprocess contracts the coordinator drives (controls, prepare,
// run, grade) with no network, no model calls and no credentials. Every
// invocation is appended to $FAKE_SWE_CALL_LOG so tests can prove exactly
// which commands ran — and that paid-model `run` commands were never replayed.
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

const log = process.env.FAKE_SWE_CALL_LOG;
if (log) fs.appendFileSync(log, JSON.stringify({ sub: process.argv[2], args: process.argv.slice(3), at: Date.now() }) + "\n");

const sub = process.argv[2];

if (sub === "controls") {
  const controlId = process.argv[3];
  const out = process.argv[4];
  if (fs.existsSync(path.join(out, "report.json"))) process.exit(1); // fresh-output contract
  for (const kind of ["noop", "gold"]) {
    const workspace = path.join(out, kind, "workspace");
    fs.mkdirSync(workspace, { recursive: true });
    fs.writeFileSync(
      path.join(workspace, "stdout.log"),
      kind === "noop" ? "models/feature_test.go:4:2: undefined: Feature\n" : "ok\n",
    );
    fs.writeFileSync(path.join(workspace, "stderr.log"), "");
  }
  fs.mkdirSync(out, { recursive: true });
  const results = [
    { kind: "noop", valid: false, resolved: false, error: null, exitCode: 1, parsedTests: 0, requiredTests: 2, requiredTestsPassed: 0 },
    { kind: "gold", valid: true, resolved: true, error: null, exitCode: 0, parsedTests: 2, requiredTests: 2, requiredTestsPassed: 2 },
  ];
  fs.writeFileSync(
    path.join(out, "report.json"),
    JSON.stringify({ ok: true, instance_id: `fake-task-${controlId}`, results }, null, 2) + "\n",
  );
  process.exit(0);
}

if (sub === "prepare") {
  const id = process.argv[3];
  const mode = process.argv[4];
  const root = process.argv[5];
  const repo = path.join(root, "work/attempts", id, "repo");
  fs.mkdirSync(repo, { recursive: true });
  // Fixed dates keep the baseline commit deterministic, so both modes of a
  // task share one recorded source tree (matchedTree) exactly like real checkouts.
  const git = (...args) =>
    execFileSync("git", ["-C", repo, ...args], {
      encoding: "utf8",
      env: { ...process.env, GIT_AUTHOR_DATE: "@1735689600 +0000", GIT_COMMITTER_DATE: "@1735689600 +0000" },
    });
  git("init", "-q");
  git("config", "user.name", "fake");
  git("config", "user.email", "fake@localhost");
  fs.writeFileSync(path.join(repo, "feature.go"), "package main\n\nfunc Before() {}\n");
  git("add", ".");
  git("commit", "-qm", "baseline");
  const baseline = git("rev-parse", "HEAD").trim();
  const baselineTree = git("rev-parse", "HEAD^{tree}").trim();
  const evidence = path.join(root, "evidence/attempts", id);
  fs.mkdirSync(evidence, { recursive: true });
  fs.writeFileSync(path.join(evidence, "ready.json"), JSON.stringify({ mode, baseline, baselineTree }, null, 2) + "\n");
  process.exit(0);
}

if (sub === "run") {
  const id = process.argv[3];
  const root = process.argv[4];
  const evidence = path.join(root, "evidence/attempts", id);
  fs.mkdirSync(evidence, { recursive: true });
  const events = [
    { type: "provider_request", number: 1, model: "fake-main" },
    { type: "provider_response", number: 1, status: 200 },
    { type: "assistant_end", model: "fake-main", stopReason: "stop", usage: { cost: { total: 0.01 } } },
    { type: "session_shutdown" },
  ];
  fs.writeFileSync(path.join(evidence, "events.jsonl"), events.map((event) => JSON.stringify(event)).join("\n") + "\n");
  const repo = path.join(root, "work/attempts", id, "repo");
  fs.writeFileSync(path.join(repo, "feature.go"), "package main\n\nfunc Feature() {}\n");
  fs.writeFileSync(path.join(repo, "scratch.txt"), "untracked\n");
  // A lock left behind by the "model" run: the coordinator must capture the
  // patch without removing it or touching the real index.
  fs.writeFileSync(path.join(repo, ".git/index.lock"), "fake worker lock\n");
  process.exit(0);
}

if (sub === "grade") {
  const index = process.argv[3];
  const patchPath = process.argv[4];
  const out = process.argv[5];
  if (fs.existsSync(path.join(out, "report.json"))) process.exit(1);
  if (!fs.existsSync(patchPath)) process.exit(1);
  const workspace = path.join(out, "solver/workspace");
  fs.mkdirSync(workspace, { recursive: true });
  fs.writeFileSync(path.join(workspace, "stdout.log"), "ok\n");
  fs.writeFileSync(path.join(workspace, "stderr.log"), "");
  const result = { kind: "solver", valid: true, resolved: true, error: null, exitCode: 0, parsedTests: 2, requiredTests: 2, requiredTestsPassed: 2 };
  fs.mkdirSync(out, { recursive: true });
  fs.writeFileSync(
    path.join(out, "report.json"),
    JSON.stringify({ ok: true, instance_id: `fake-task-${index}`, results: [result] }, null, 2) + "\n",
  );
  process.exit(0);
}

console.error(`fake-swe-harness: unknown subcommand ${JSON.stringify(sub)}`);
process.exit(2);
