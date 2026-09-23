#!/usr/bin/env node
// Config-driven coordinator for native SWE benchmark attempts.
//
// Wraps the existing native worker/grader subprocesses (any commands you
// configure) with the repaired contracts:
//   * canonical no-op/gold control verdicts gate every paid call;
//   * patches are captured through the lock-safe isolated index instead of
//     `git add -N` on the shared working index;
//   * request lifecycle accounting distinguishes settled usage from HTTP
//     response headers and holds a conservative reserve for unsettled ones;
//   * write-ahead checkpoints make every started attempt unreplayable —
//     finished-but-ungraded work is recovered offline (prewalk-swe-recover),
//     never re-run;
//   * exits nonzero with an actionable state for needs-attention/budget stops.
//
// Nothing here talks to providers directly; the config names the commands.
// usage: node bench/prewalk/prewalk-swe-run.mjs --config <file> [--resume]

import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import {
  analyzeRequestLifecycle,
  classifyCandidate,
  classifyControls,
  comparePairs,
  planResume,
  summarizeAttempts,
} from "./lib/prewalk-swe-evidence.mjs";
import { capturePatch } from "./lib/prewalk-patch.mjs";

const argv = process.argv.slice(2);
const configIndex = argv.indexOf("--config");
const configPath = configIndex >= 0 ? argv[configIndex + 1] : undefined;
const resume = argv.includes("--resume");
const fail = (message, code = 1) => {
  console.error(JSON.stringify({ ok: false, error: message }));
  process.exit(code);
};
if (!configPath) fail("usage: node bench/prewalk/prewalk-swe-run.mjs --config <file> [--resume]", 2);
if (!fs.existsSync(configPath)) fail(`config not found: ${configPath}`, 2);
const config = JSON.parse(fs.readFileSync(configPath, "utf8"));

if (!config.root || !path.isAbsolute(config.root)) fail("config.root must be an absolute path");
if (!Array.isArray(config.schedule) || config.schedule.length === 0) fail("config.schedule must be a non-empty array");
const ids = config.schedule.map((entry) => entry?.id);
if (ids.some((id) => typeof id !== "string") || new Set(ids).size !== ids.length) {
  fail("config.schedule entries need unique string ids");
}
for (const entry of config.schedule) {
  if (!Number.isInteger(entry.index) || entry.index < 0) fail(`schedule entry ${entry.id} needs a non-negative integer index`);
  if (typeof entry.mode !== "string") fail(`schedule entry ${entry.id} needs a mode string`);
}
for (const name of ["controls", "prepare", "run", "grade"]) {
  const spec = config.commands?.[name];
  if (!spec || !Array.isArray(spec.argv) || spec.argv.length === 0) fail(`config.commands.${name} needs a non-empty argv array`);
  if (spec.timeoutMs !== undefined && (!Number.isInteger(spec.timeoutMs) || spec.timeoutMs <= 0)) {
    fail(`config.commands.${name}.timeoutMs must be a positive integer`);
  }
  if (spec.killGraceMs !== undefined && (!Number.isInteger(spec.killGraceMs) || spec.killGraceMs <= 0)) {
    fail(`config.commands.${name}.killGraceMs must be a positive integer`);
  }
}
const budget = config.budget ?? {};
for (const name of ["maximumTotalUsd", "priorKnownUsd", "priorUnknownWorstCaseUsd"]) {
  if (!Number.isFinite(budget[name]) || budget[name] < 0) fail(`config.budget.${name} must be a non-negative number`);
}
if (!budget.maximumRequestUsd || typeof budget.maximumRequestUsd !== "object") {
  fail("config.budget.maximumRequestUsd must map model ids to conservative caps");
}

const root = config.root;
const paths = {
  attemptsEvidence: config.paths?.attemptsEvidence ?? "evidence/attempts/{id}",
  attemptsWork: config.paths?.attemptsWork ?? "work/attempts/{id}",
  events: config.paths?.events ?? "evidence/attempts/{id}/events.jsonl",
  ready: config.paths?.ready ?? "evidence/attempts/{id}/ready.json",
  controlsDir: config.paths?.controlsDir ?? "evidence/controls/{controlId}",
  controlVerdicts: config.paths?.controlVerdicts ?? "evidence/control-verdicts.json",
  results: config.paths?.results ?? "evidence/results-new.json",
  checkpoints: config.paths?.checkpoints ?? "evidence/checkpoints.json",
  summary: config.paths?.summary ?? "evidence/summary.json",
};
fs.mkdirSync(path.join(root, "evidence"), { recursive: true });
fs.mkdirSync(path.join(root, "work"), { recursive: true });

const substitute = (text, tokens) =>
  String(text).replace(/\{([a-zA-Z]+)\}/g, (whole, key) => {
    if (!Object.prototype.hasOwnProperty.call(tokens, key)) fail(`unknown config token ${whole} in commands/paths`);
    return tokens[key];
  });
const resolve = (template, tokens) => path.resolve(root, substitute(template, tokens));
const readJsonIfExists = (file) => (fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, "utf8")) : null);
// Durable replacement: the JSON must not be observable as a torn file, and a
// crash after the rename must not lose it. The temporary file is unique and
// created exclusively in the same directory, so a concurrent or abandoned
// writer cannot truncate the one being written, and it is removed if the
// write, sync or rename fails. The parent directory is synced too on POSIX,
// because that is what makes the rename itself durable; Windows has no
// directory fsync, so there the file sync plus atomic replacement is the
// strongest guarantee available (documented in bench/prewalk/swe.md).
const atomicWrite = (file, data) => {
  const temp = path.join(
    path.dirname(file),
    `.${path.basename(file)}.${process.pid}.${Math.random().toString(36).slice(2, 10)}.tmp`,
  );
  try {
    const fd = fs.openSync(temp, "wx", 0o600);
    try {
      fs.writeFileSync(fd, JSON.stringify(data, null, 2) + "\n");
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    fs.renameSync(temp, file);
    if (process.platform === "win32") return;
    let dirFd = null;
    try {
      dirFd = fs.openSync(path.dirname(file), "r");
      fs.fsyncSync(dirFd);
    } catch {
      /* some filesystems reject a directory fsync; the rename already happened */
    } finally {
      if (dirFd !== null) fs.closeSync(dirFd);
    }
  } catch (error) {
    try {
      fs.rmSync(temp, { force: true });
    } catch {
      /* nothing left to remove */
    }
    throw error;
  }
};

const resultsPath = resolve(paths.results, {});
const checkpointsPath = resolve(paths.checkpoints, {});
const controlVerdictsPath = resolve(paths.controlVerdicts, {});
const summaryPath = resolve(paths.summary, {});
const rows = readJsonIfExists(resultsPath) ?? [];
const checkpoints = readJsonIfExists(checkpointsPath) ?? {};
const controlVerdicts = readJsonIfExists(controlVerdictsPath) ?? {};

const checkpointList = Object.entries(checkpoints).map(([id, checkpoint]) => ({
  id,
  phase: checkpoint?.phase ?? "unknown",
}));
if (checkpointList.length > 0 && !resume) {
  fail(
    `checkpoints exist for [${checkpointList.map((c) => c.id).join(", ")}]; rerun with --resume to continue without replaying started attempts`,
    2,
  );
}
const plan = planResume(config.schedule, checkpointList);

const state = { startedAt: new Date().toISOString(), terminal: null, reason: null };
const settleStop = (terminal, reason) => {
  state.terminal = terminal;
  state.reason = reason;
};

const availableUsd = () =>
  budget.maximumTotalUsd -
  budget.priorKnownUsd -
  budget.priorUnknownWorstCaseUsd -
  rows.reduce((total, row) => total + (row.usageUsd ?? 0) + (row.usageHoldUsd ?? 0), 0);

const runCommand = (spec, tokens, logFile) =>
  new Promise((resolveRun) => {
    const commandArgv = spec.argv.map((part) => substitute(part, tokens));
    const cwd = spec.cwd ? substitute(spec.cwd, tokens) : root;
    const env = { ...process.env };
    for (const [key, entry] of Object.entries(spec.env ?? {})) env[key] = substitute(String(entry), tokens);
    const started = Date.now();
    const timeoutMs = spec.timeoutMs ?? 600_000;
    const killGraceMs = spec.killGraceMs ?? 30_000;
    fs.mkdirSync(path.dirname(logFile), { recursive: true });
    const fd = fs.openSync(logFile, "a", 0o600);
    // A detached POSIX child leads its own process group, so a timeout can
    // reach grandchildren that would otherwise outlive the leader.
    const child = spawn(commandArgv[0], commandArgv.slice(1), {
      cwd,
      env,
      stdio: ["ignore", fd, fd],
      detached: process.platform !== "win32",
    });
    let timedOut = false;
    let settled = false;
    let closeResult = null;
    let terminateTimer = null;
    const killTree = (signal) => {
      if (typeof child.pid !== "number") return;
      if (process.platform === "win32") {
        // Windows has no POSIX process groups; taskkill /T walks the child tree.
        // The grace period is preserved by terminating the tree without /F and
        // forcing it only on the escalation signal.
        const args = ["/PID", String(child.pid), "/T"];
        if (signal === "SIGKILL") args.push("/F");
        try {
          spawnSync("taskkill", args, { stdio: "ignore" });
        } catch {
          /* tree already gone */
        }
        return;
      }
      try {
        process.kill(-child.pid, signal);
      } catch {
        try {
          child.kill(signal);
        } catch {
          /* already reaped */
        }
      }
    };
    const outcome = () =>
      closeResult ?? { code: null, signal: null, timedOut, durationSeconds: (Date.now() - started) / 1000 };
    // spawn() failures emit `error` and then `close`, so cleanup and resolution
    // happen exactly once, from whichever handler reports the outcome first.
    const finish = (result) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (terminateTimer !== null) clearTimeout(terminateTimer);
      try {
        fs.closeSync(fd);
      } catch {
        /* already closed */
      }
      resolveRun(result);
    };
    const timer = setTimeout(() => {
      timedOut = true;
      killTree("SIGTERM");
      // Escalation runs on the clock, never on `close`: a leader that exits on
      // SIGTERM can leave descendants holding the log descriptor.
      terminateTimer = setTimeout(() => {
        killTree("SIGKILL");
        terminateTimer = setTimeout(() => finish(outcome()), 100);
      }, killGraceMs);
    }, timeoutMs);
    child.on("error", (error) => {
      closeResult = {
        code: null,
        signal: null,
        timedOut,
        durationSeconds: (Date.now() - started) / 1000,
        error: String(error?.message ?? error),
      };
      if (!timedOut) finish(closeResult);
    });
    child.on("close", (code, signal) => {
      closeResult = { code, signal, timedOut, durationSeconds: (Date.now() - started) / 1000 };
      if (!timedOut) finish(closeResult);
    });
  });

const readExecutionLogs = (gradeDir) => ({
  stdout: fs.existsSync(path.join(gradeDir, "solver/workspace/stdout.log"))
    ? fs.readFileSync(path.join(gradeDir, "solver/workspace/stdout.log"), "utf8")
    : "",
  stderr: fs.existsSync(path.join(gradeDir, "solver/workspace/stderr.log"))
    ? fs.readFileSync(path.join(gradeDir, "solver/workspace/stderr.log"), "utf8")
    : "",
});
const solverResultFrom = (report) => (report?.results ?? []).find((result) => result.kind === "solver") ?? null;

if (plan.needsRecovery.length > 0) {
  settleStop(
    "needs-attention",
    `started-but-unfinished attempts require offline recovery (prewalk-swe-recover), never a re-run: [${plan.needsRecovery.map((e) => e.id).join(", ")}]`,
  );
}

const executed = [];
if (!state.terminal) {
  for (const entry of plan.runnable) {
    if (availableUsd() <= 0) {
      settleStop("budget-stopped", "monitored budget exhausted before the next attempt");
      break;
    }
    const tokens = {
      root,
      id: entry.id,
      index: String(entry.index),
      mode: entry.mode,
      controlId: String(entry.index + 1).padStart(3, "0"),
    };
    const controlId = tokens.controlId;
    const evidenceDir = resolve(paths.attemptsEvidence, tokens);
    const attemptsWorkDir = resolve(paths.attemptsWork, tokens);

    // 1. canonical preflight controls (cached per task)
    let controlsPassed = controlVerdicts[controlId]?.ok === true;
    if (!controlsPassed) {
      const controlsOut = resolve(paths.controlsDir, tokens);
      const reportPath = path.join(controlsOut, "report.json");
      try {
        if (!fs.existsSync(reportPath)) {
          const controlsRun = await runCommand(
            config.commands.controls,
            { ...tokens, controlsOut },
            `${evidenceDir}-controls.log`,
          );
          if (controlsRun.code !== 0 && !fs.existsSync(reportPath)) {
            throw new Error(`controls command exited ${JSON.stringify(controlsRun.code)} without a report`);
          }
        }
        const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
        const arm = (kind) => {
          const result = (report.results ?? []).find((candidate) => candidate.kind === kind);
          if (!result) return null;
          return {
            result: {
              valid: result.valid,
              resolved: result.resolved,
              error: result.error ?? null,
              parsedTests: result.parsedTests ?? 0,
              requiredTests: result.requiredTests ?? 0,
              requiredTestsPassed: result.requiredTestsPassed ?? 0,
            },
            execution: {
              setupOk: result.error == null && result.exitCode != null,
              testStarted: result.exitCode != null && result.error == null,
              testExitCode: result.exitCode ?? null,
              stdout: fs.existsSync(path.join(controlsOut, kind, "workspace/stdout.log"))
                ? fs.readFileSync(path.join(controlsOut, kind, "workspace/stdout.log"), "utf8")
                : "",
              stderr: fs.existsSync(path.join(controlsOut, kind, "workspace/stderr.log"))
                ? fs.readFileSync(path.join(controlsOut, kind, "workspace/stderr.log"), "utf8")
                : "",
            },
          };
        };
        const verdict = classifyControls({ noop: arm("noop"), gold: arm("gold") });
        controlVerdicts[controlId] = { at: new Date().toISOString(), ...verdict };
        atomicWrite(controlVerdictsPath, controlVerdicts);
        controlsPassed = verdict.ok;
      } catch (error) {
        controlVerdicts[controlId] = { at: new Date().toISOString(), present: true, ok: false, error: String(error?.message ?? error) };
        atomicWrite(controlVerdictsPath, controlVerdicts);
      }
    }
    if (!controlsPassed) {
      rows.push({
        id: entry.id,
        index: entry.index,
        mode: entry.mode,
        modelRequests: 0,
        failureKind: "grader-controls",
        error: "canonical control verdict failed; no model call made",
      });
      atomicWrite(resultsPath, rows);
      executed.push(entry.id);
      continue;
    }

    // 2. prepare (no paid calls)
    const prepareRun = await runCommand(config.commands.prepare, tokens, evidenceDir + "-prepare.log");
    if (prepareRun.code !== 0) {
      rows.push({ id: entry.id, index: entry.index, mode: entry.mode, modelRequests: 0, failureKind: "prepare", error: `exit ${JSON.stringify(prepareRun.code)}` });
      atomicWrite(resultsPath, rows);
      executed.push(entry.id);
      continue;
    }
    const ready = readJsonIfExists(resolve(paths.ready, tokens));
    if (!ready?.baseline) {
      rows.push({ id: entry.id, index: entry.index, mode: entry.mode, modelRequests: 0, failureKind: "ready-missing", error: "no ready.json with a baseline commit" });
      atomicWrite(resultsPath, rows);
      executed.push(entry.id);
      continue;
    }
    if (ready.mode !== undefined && ready.mode !== entry.mode) {
      settleStop("needs-attention", `attempt ${entry.id} prepared mode ${ready.mode} but was scheduled as ${entry.mode}`);
      break;
    }

    // 3. write-ahead: this attempt may never be issued again
    checkpoints[entry.id] = { phase: "starting", updatedAt: new Date().toISOString() };
    atomicWrite(checkpointsPath, checkpoints);

    // 4. paid run
    const runResult = await runCommand(config.commands.run, tokens, evidenceDir + "-runner.log");
    const eventsPath = resolve(paths.events, tokens);
    let lifecycle = null;
    if (!fs.existsSync(eventsPath)) {
      settleStop("needs-attention", `attempt ${entry.id} finished without request evidence (${eventsPath} missing); recover offline`);
      atomicWrite(resultsPath, rows);
      executed.push(entry.id);
      break;
    }
    const events = [];
    let eventsError = null;
    for (const [index, line] of fs.readFileSync(eventsPath, "utf8").split("\n").entries()) {
      if (!line.trim()) continue;
      try {
        events.push(JSON.parse(line));
      } catch {
        eventsError = `malformed events line ${index + 1}`;
        break;
      }
    }
    if (eventsError) {
      settleStop("needs-attention", `attempt ${entry.id}: ${eventsError}; usage accounting is uncertain`);
      atomicWrite(resultsPath, rows);
      executed.push(entry.id);
      break;
    }
    try {
      lifecycle = analyzeRequestLifecycle(events, { maximumRequestUsd: budget.maximumRequestUsd });
    } catch (error) {
      settleStop("needs-attention", `attempt ${entry.id}: ${String(error?.message ?? error)}`);
      atomicWrite(resultsPath, rows);
      executed.push(entry.id);
      break;
    }
    const modelRequests = events.filter((event) => event.type === "provider_request").length;
    if (modelRequests === 0) {
      settleStop("needs-attention", `attempt ${entry.id} recorded no provider requests; usage evidence incomplete`);
      atomicWrite(resultsPath, rows);
      executed.push(entry.id);
      break;
    }

    // 5. lock-safe patch capture
    let captured;
    try {
      captured = await capturePatch({
        repo: path.join(attemptsWorkDir, "repo"),
        baseline: ready.baseline,
        out: path.join(evidenceDir, "capture"),
      });
    } catch (error) {
      settleStop("needs-attention", `attempt ${entry.id}: patch capture failed: ${String(error?.message ?? error)}`);
      atomicWrite(resultsPath, rows);
      executed.push(entry.id);
      break;
    }

    // 6. offline grading
    const gradeOut = path.join(evidenceDir, "grading");
    const gradeRun = await runCommand(
      config.commands.grade,
      { ...tokens, patchPath: captured.patchPath, gradeOut },
      evidenceDir + "-grade.log",
    );
    const gradeReportPath = path.join(gradeOut, "report.json");
    let verdict;
    if (gradeRun.code !== 0 || !fs.existsSync(gradeReportPath)) {
      verdict = {
        status: "unobserved",
        validComparison: false,
        reason: `grade command exited ${JSON.stringify(gradeRun.code)}; recoverable offline`,
      };
    } else {
      const report = JSON.parse(fs.readFileSync(gradeReportPath, "utf8"));
      const gradeResult = solverResultFrom(report);
      const logs = readExecutionLogs(gradeOut);
      verdict = classifyCandidate({
        result: gradeResult,
        controlsPassed,
        execution: {
          setupOk: gradeResult?.error == null && gradeResult?.exitCode != null,
          testStarted: gradeResult?.exitCode != null && gradeResult?.error == null,
          testExitCode: gradeResult?.exitCode ?? null,
          stdout: logs.stdout,
          stderr: logs.stderr,
        },
      });
    }

    rows.push({
      id: entry.id,
      index: entry.index,
      mode: entry.mode,
      modelRequests,
      usageUsd: lifecycle.knownUsd,
      usageHoldUsd: lifecycle.heldUsd,
      timedOut: runResult.timedOut,
      solverDurationSeconds: runResult.durationSeconds,
      baselineTree: ready.baselineTree ?? null,
      resolved: verdict.status === "pass",
      validGrade: verdict.validComparison,
      verdict,
    });
    // The row lands first: a crash before the finished checkpoint leaves the
    // attempt at `starting`, which planResume routes to offline recovery
    // instead of dropping paid work whose result was never recorded.
    atomicWrite(resultsPath, rows);
    checkpoints[entry.id] = { phase: "finished", updatedAt: new Date().toISOString() };
    atomicWrite(checkpointsPath, checkpoints);
    executed.push(entry.id);

    if (verdict.status === "unobserved" && !runResult.timedOut) {
      settleStop("needs-attention", `attempt ${entry.id}: ${verdict.reason}`);
      break;
    }
    if (lifecycle.uncertain && !runResult.timedOut) {
      settleStop("needs-attention", `attempt ${entry.id}: unsettled provider usage; recover offline before continuing`);
      break;
    }
  }
}

const counts = summarizeAttempts(rows);
const comparison = comparePairs(rows.filter((row) => (row.modelRequests ?? 0) > 0));
const finalState = state.terminal ?? (plan.needsRecovery.length > 0 ? "needs-attention" : executed.length === plan.runnable.length ? "finished" : "stopped");
atomicWrite(summaryPath, {
  at: new Date().toISOString(),
  state: finalState,
  reason: state.reason,
  planned: config.schedule.length,
  runnable: plan.runnable.map((entry) => entry.id),
  needsRecovery: plan.needsRecovery.map((entry) => entry.id),
  completed: plan.completed.map((entry) => entry.id),
  executedThisRun: executed,
  counts,
  comparison: {
    comparablePairs: comparison.comparablePairs,
    bothResolved: comparison.bothResolved,
    astraOnlyResolved: comparison.astraOnlyResolved,
    prewalkOnlyResolved: comparison.prewalkOnlyResolved,
    bothUnresolved: comparison.bothUnresolved,
    costIntervalUsd: comparison.costIntervalUsd,
    meanSuccessfulSolverSeconds: comparison.meanSuccessfulSolverSeconds,
  },
  budget: {
    maximumTotalUsd: budget.maximumTotalUsd,
    availableUsd: Math.max(0, availableUsd()),
    hardCapGuaranteed: false,
  },
});
console.log(JSON.stringify({ ok: finalState === "finished", state: finalState, executed, counts }));
process.exitCode = finalState === "finished" ? 0 : 1;
