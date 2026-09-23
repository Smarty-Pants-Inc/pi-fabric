#!/usr/bin/env node
// Minimal single-cell canary runner for future Prewalk comparisons.
//
// Spawns a fresh root `pi --mode json` process with the maintained telemetry
// extension (bench/prewalk/prewalk-canary-telemetry.ts) and, by default, the compiled
// dogfood extension at the repo root. It records the raw JSON stdout plus a
// parallel arrival-timestamp log — the only place a too-small compaction
// attempt start is observable, because compaction_start precedes
// session_before_compact — preserves final unterminated stdout bytes, and
// never replays a completed cell: a refusing started.json is the replay
// guard.
//
// Any nonzero pi exit, timeout, truncated stdout line, malformed telemetry,
// or missing session_shutdown record fails the run and is recorded verbatim
// in finished.json; the runner then exits nonzero itself.
//
// The default JSON mode is one-shot: `pi --mode json` exits as soon as its
// prompt settles, which abandons a recovery message an extension queued during
// settlement. Opt-in --rpc keeps a persistent session open instead: the prompt
// is sent as an RPC command and stdin stays open until --rpc-runs agent runs
// have settled AND get_state reports idle with an empty queue, then the runner
// sends EOF for a graceful shutdown.
//
// usage: node bench/prewalk/prewalk-canary-run.mjs --out <dir> --cwd <dir> --prompt-file <file> \
//        [--model <provider/id>] [--thinking <level>] [--timeout-seconds 300] \
//        [--rpc] [--rpc-runs 2] [--turns 16] [--abort-grace-seconds <seconds>] [--request-contract <file>] \
//        [--task-check <spec.json>] \
//        [--pi-binary <path>] [--extension <path>]... [--pi-arg <value>]...

import fs from "node:fs";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { StringDecoder } from "node:string_decoder";
import { fileURLToPath } from "node:url";
import { sha256, writeExclusive } from "./lib/prewalk-bench-lib.mjs";
import { parseRequestContract, parseTaskCheckSpec } from "./lib/prewalk-live-evidence.mjs";

const argv = process.argv.slice(2);
const value = (flag) => {
  const index = argv.indexOf(flag);
  return index >= 0 ? argv[index + 1] : undefined;
};
const repeated = (flag) => {
  const values = [];
  for (let index = 0; index < argv.length; index += 1) {
    if (argv[index] === flag && argv[index + 1] !== undefined) values.push(argv[index + 1]);
  }
  return values;
};

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const recorderPath = path.join(repoRoot, "bench", "prewalk", "prewalk-canary-telemetry.ts");
const RUNTIME_FILES = ["dist/index.js", "dist/fabric-runtime-state.js", "bench/prewalk/prewalk-canary-telemetry.ts"];

const fail = (error) => {
  console.error(JSON.stringify({ ok: false, error }));
  process.exit(1);
};

const outDirArg = value("--out");
const cwdArg = value("--cwd");
const promptFile = value("--prompt-file");
const model = value("--model");
const thinking = value("--thinking");
const piBinary = value("--pi-binary") ?? "pi";
const timeoutSeconds = Number(value("--timeout-seconds") ?? 300);
const rpcMode = argv.includes("--rpc");
const turns = Number(value("--turns") ?? 16);
const rpcRuns = Number(value("--rpc-runs") ?? 2);
const abortGraceSeconds = Number(value("--abort-grace-seconds") ?? 0);
const requestContract = value("--request-contract");
const taskCheck = value("--task-check");
if (!outDirArg || !cwdArg || !promptFile) {
  console.error(
    "usage: node bench/prewalk/prewalk-canary-run.mjs --out <dir> --cwd <dir> --prompt-file <file> " +
      "[--model <provider/id>] [--thinking <level>] [--timeout-seconds 300] " +
      "[--abort-grace-seconds <seconds>] " +
      "[--pi-binary <path>] [--extension <path>]... [--pi-arg <value>]... " +
      "[--request-contract <file>] [--task-check <spec.json>]",
  );
  process.exit(2);
}
if (![outDirArg, cwdArg, promptFile].every((candidate) => path.isAbsolute(candidate))) {
  fail("--out, --cwd, and --prompt-file must be absolute paths");
}
// Every containment and provenance comparison uses resolved paths: a trailing
// separator on --cwd or --out must not turn a valid child into an escaping one.
const cwd = path.resolve(cwdArg);
const outDir = path.resolve(outDirArg);
if (!Number.isInteger(timeoutSeconds) || timeoutSeconds <= 0) {
  fail("--timeout-seconds must be a positive integer");
}
if (!Number.isInteger(turns) || turns <= 0) {
  fail("--turns must be a positive integer");
}
if (!Number.isInteger(rpcRuns) || rpcRuns <= 0) {
  fail("--rpc-runs must be a positive integer");
}
if (!Number.isInteger(abortGraceSeconds) || abortGraceSeconds < 0) {
  fail("--abort-grace-seconds must be a non-negative integer");
}
if (abortGraceSeconds > 0 && !rpcMode) {
  fail("--abort-grace-seconds requires --rpc: only RPC mode has an abort command");
}
// Provider/model extensions are additions: the compiled dogfood runtime and
// the maintained recorder always stay loaded.
const extraExtensions = repeated("--extension");
if (extraExtensions.some((extension) => !path.isAbsolute(extension))) {
  fail("--extension paths must be absolute");
}
if (!fs.existsSync(cwd) || !fs.lstatSync(cwd).isDirectory()) {
  fail(`--cwd is not an existing directory: ${cwd}`);
}
if (!fs.existsSync(promptFile) || !fs.lstatSync(promptFile).isFile()) {
  fail(`--prompt-file is not an existing file: ${promptFile}`);
}
if (requestContract !== undefined) {
  if (!path.isAbsolute(requestContract) || !fs.existsSync(requestContract) || !fs.lstatSync(requestContract).isFile()) {
    fail(`--request-contract is not an existing absolute file: ${requestContract}`);
  }
  try {
    parseRequestContract(fs.readFileSync(requestContract, "utf8"), requestContract);
  } catch (error) {
    fail(String(error?.message ?? error));
  }
}
if (fs.existsSync(path.join(outDir, "started.json"))) {
  fail("cell already started; inspect evidence, never replay it");
}

const prompt = fs.readFileSync(promptFile, "utf8");
// The spec only names relative paths inside --cwd, so task verification can
// never become an execution channel for the cell under test: the runner always
// issues one fixed `node --test` invocation over the named file.
let taskCheckSpec = null;
if (taskCheck !== undefined) {
  if (!path.isAbsolute(taskCheck) || !fs.existsSync(taskCheck) || !fs.lstatSync(taskCheck).isFile()) {
    fail(`--task-check is not an existing absolute file: ${taskCheck}`);
  }
  try {
    const spec = parseTaskCheckSpec(fs.readFileSync(taskCheck, "utf8"), taskCheck);
    const insideCwd = (relative) => {
      const resolved = path.resolve(cwd, relative);
      if (resolved !== cwd && !resolved.startsWith(cwd + path.sep)) {
        fail(`--task-check path escapes --cwd: ${relative}`);
      }
      return resolved;
    };
    const files = { testFile: insideCwd(spec.testFile), artifact: insideCwd(spec.artifact) };
    for (const [label, file] of Object.entries(files)) {
      if (!fs.existsSync(file) || !fs.lstatSync(file).isFile()) {
        fail(`--task-check ${label} is not an existing file: ${file}`);
      }
    }
    taskCheckSpec = {
      path: path.resolve(taskCheck),
      sha256: sha256(fs.readFileSync(taskCheck)),
      testFile: spec.testFile,
      artifact: spec.artifact,
    };
  } catch (error) {
    fail(String(error?.message ?? error));
  }
}

const hashInsideCwd = (relative) => {
  const resolved = path.resolve(cwd, relative);
  if (resolved !== cwd && !resolved.startsWith(cwd + path.sep)) return { sha256: null, sizeBytes: null };
  if (!fs.existsSync(resolved) || !fs.lstatSync(resolved).isFile()) return { sha256: null, sizeBytes: null };
  const bytes = fs.readFileSync(resolved);
  return { sha256: sha256(bytes), sizeBytes: bytes.length };
};

const runTaskCheck = (spec) => {
  const startedAtCheck = new Date().toISOString();
  const before = hashInsideCwd(spec.artifact);
  const args = ["--test", "--test-reporter=tap", spec.testFile];
  const result = spawnSync(process.execPath, args, { cwd, encoding: "utf8", timeout: 300_000 });
  const output = `${result.stdout ?? ""}\n${result.stderr ?? ""}`;
  const count = (label) => {
    const match = output.match(new RegExp(`^# ${label} (\\d+)$`, "m"));
    return match ? Number(match[1]) : null;
  };
  const counts = {
    tests: count("tests"),
    pass: count("pass"),
    fail: count("fail"),
    skipped: count("skipped"),
    todo: count("todo"),
  };
  const after = hashInsideCwd(spec.artifact);
  return {
    spec: { path: spec.path, sha256: spec.sha256 },
    cwd,
    testFile: spec.testFile,
    command: [process.execPath, ...args],
    exitCode: result.status ?? null,
    signal: result.signal ?? null,
    counts,
    countsObserved: counts.tests !== null && counts.pass !== null && counts.fail !== null,
    artifact: {
      path: spec.artifact,
      sha256Before: before.sha256,
      sha256After: after.sha256,
      sizeBytes: after.sizeBytes,
      unchangedDuringCheck: before.sha256 !== null && before.sha256 === after.sha256,
    },
    ok: result.status === 0 && counts.fail === 0 && (counts.tests ?? 0) > 0,
    startedAt: startedAtCheck,
    finishedAt: new Date().toISOString(),
  };
};

fs.mkdirSync(outDir, { recursive: true });
const extensions = [repoRoot, recorderPath, ...extraExtensions];

const runtimeHashes = {};
for (const rel of RUNTIME_FILES) {
  const file = path.join(repoRoot, rel);
  runtimeHashes[rel] = fs.existsSync(file) ? sha256(fs.readFileSync(file)) : null;
}

const env = {
  ...process.env,
  PREWALK_CANARY_TELEMETRY: path.join(outDir, "telemetry.jsonl"),
  PREWALK_CANARY_MAX_TURNS: String(turns),
  PI_SKIP_VERSION_CHECK: "1",
  PI_TELEMETRY: "0",
};
for (const key of Object.keys(env)) {
  if (
    key.startsWith("PI_FABRIC_") ||
    ["PI_SESSION_ID", "PI_SESSION_FILE", "PI_PROVIDER", "PI_MODEL", "PI_REASONING_LEVEL", "PREWALK_CANARY_REQUEST_CONTRACT"].includes(key)
  ) {
    delete env[key];
  }
}
if (requestContract !== undefined) env.PREWALK_CANARY_REQUEST_CONTRACT = requestContract;

const startedAt = Date.now();
writeExclusive(
  path.join(outDir, "started.json"),
  JSON.stringify(
    {
      startedAt: new Date(startedAt).toISOString(),
      cwd,
      piBinary,
      model: model ?? null,
      thinking: thinking ?? null,
      promptSha256: sha256(prompt),
      promptBytes: Buffer.byteLength(prompt),
      timeoutSeconds,
      abortGraceSeconds,
      mode: rpcMode ? "rpc" : "json",
      turns,
      rpcRuns: rpcMode ? rpcRuns : null,
      extensions,
      piArgs: repeated("--pi-arg"),
      requestContract: requestContract === undefined ? null : { path: requestContract, sha256: sha256(fs.readFileSync(requestContract)) },
      taskCheck: taskCheckSpec === null ? null : { path: taskCheckSpec.path, sha256: taskCheckSpec.sha256, testFile: taskCheckSpec.testFile, artifact: taskCheckSpec.artifact },
      runtimeHashes,
    },
    null,
    2,
  ) + "\n",
);

const piArgs = [
  "--approve",
  "--mode",
  rpcMode ? "rpc" : "json",
  "--offline",
  "--no-extensions",
  ...extensions.flatMap((extension) => ["-e", extension]),
  "--no-skills",
  "--no-context-files",
  "--no-prompt-templates",
  "--no-themes",
  ...(model ? ["--model", model] : []),
  ...(thinking ? ["--thinking", thinking] : []),
  "--session-dir",
  path.join(outDir, "sessions"),
  ...repeated("--pi-arg"),
  ...(rpcMode ? [] : [prompt]),
];
const child = spawn(piBinary, piArgs, {
  cwd,
  env,
  stdio: [rpcMode ? "pipe" : "ignore", "pipe", "pipe"],
  detached: true,
});
const eventsFd = fs.openSync(path.join(outDir, "events.jsonl"), "wx");
const arrivalFd = fs.openSync(path.join(outDir, "arrival.jsonl"), "wx");
const stderrFd = fs.openSync(path.join(outDir, "stderr.log"), "wx");

// RPC control state. Every stdout line is still written verbatim to
// events.jsonl; responses are additionally correlated by id for control.
let settledRuns = 0;
let closed = false;
let rpcId = 0;
const rpcPending = new Map();
const rpcCommands = [];
const rpcProblems = [];
if (rpcMode) child.stdin.on("error", () => { /* EPIPE after an early child exit */ });

const rpcRequest = (command) => {
  const id = `runner-${++rpcId}`;
  const record = { id, command: command.type, success: null, error: null };
  rpcCommands.push(record);
  return new Promise((resolve) => {
    const deliver = (response) => {
      if (!rpcPending.delete(id)) return;
      if (response === null) record.success = record.success === true;
      else {
        record.success = response.success === true;
        if (response.error !== undefined) record.error = String(response.error);
      }
      resolve(response);
    };
    rpcPending.set(id, deliver);
    if (closed) {
      record.success = false;
      record.error = "stdin closed";
      deliver(null);
      return;
    }
    try {
      child.stdin.write(JSON.stringify({ id, ...command }) + "\n");
    } catch (error) {
      record.success = false;
      record.error = String(error);
      deliver(null);
    }
  });
};
const handleRpcLine = (line, lineNumber) => {
  let parsed;
  try {
    parsed = JSON.parse(line);
  } catch {
    rpcProblems.push(`malformed rpc stdout line ${lineNumber + 1}`);
    return;
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    rpcProblems.push(`malformed rpc stdout line ${lineNumber + 1}: not an object`);
    return;
  }
  if (parsed.type === "response") {
    const deliver = rpcPending.get(parsed.id);
    if (deliver) deliver(parsed);
    else rpcProblems.push(`unexpected rpc response id ${JSON.stringify(parsed.id)}`);
    return;
  }
  if (parsed.type === "agent_settled") settledRuns += 1;
};
const sleep = (ms) => new Promise((resolve) => {
  const handle = setTimeout(resolve, ms);
  handle.unref?.();
});
const until = async (predicate) => {
  for (;;) {
    if (predicate()) return true;
    if (closed || timedOut) return false;
    await sleep(50);
  }
};

let lineIndex = 0;
let tail = Buffer.alloc(0);
const decoder = new StringDecoder("utf8");
child.stdout.on("data", (chunk) => {
  tail = Buffer.concat([tail, chunk]);
  let newline = tail.indexOf(0x0a);
  while (newline >= 0) {
    const lineBuffer = tail.subarray(0, newline);
    tail = tail.subarray(newline + 1);
    const line = decoder.write(lineBuffer);
    fs.writeSync(eventsFd, line + "\n");
    fs.writeSync(arrivalFd, `${JSON.stringify({ lineIndex, arrivalMs: Date.now() })}\n`);
    if (rpcMode) handleRpcLine(line, lineIndex);
    lineIndex += 1;
    newline = tail.indexOf(0x0a);
  }
});
child.stderr.on("data", (chunk) => fs.writeSync(stderrFd, chunk));

let timedOut = false;
let killTimer;
// Opt-in graceful abort (RPC only): the timeout first asks pi to abort through
// its documented RPC command and waits a bounded grace for a clean exit, then
// escalates to the original hard process-group termination. A timeout still
// fails the cell either way; the grace only preserves a clean partial record.
const gracefulAbort = { attempted: false, response: null, settled: false };
const stopGroup = async () => {
  timedOut = true;
  if (rpcMode && abortGraceSeconds > 0 && !closed) {
    gracefulAbort.attempted = true;
    // A ref'd no-op timer keeps the loop alive for the grace window: sleep()
    // timers are unref'd, so without this the process would exit with
    // unfinished top-level awaits the moment the child closes.
    const graceTimer = setTimeout(() => {}, abortGraceSeconds * 1000);
    try {
      const response = await rpcRequest({ type: "abort" });
      gracefulAbort.response =
        response === null ? "no response" : response.success === true ? "accepted" : String(response.error ?? "rejected");
      if (response?.success === true) {
        const deadline = Date.now() + abortGraceSeconds * 1000;
        while (!closed && Date.now() < deadline) await sleep(50);
        gracefulAbort.settled = closed;
      }
    } catch (error) {
      gracefulAbort.response = String(error?.message ?? error);
    } finally {
      clearTimeout(graceTimer);
    }
  }
  if (closed) return;
  try {
    process.kill(-child.pid, "SIGTERM");
  } catch {
    /* group already gone */
  }
  killTimer = setTimeout(() => {
    try {
      process.kill(-child.pid, "SIGKILL");
    } catch {
      /* group already gone */
    }
  }, 5000);
};
let stopPromise = null;
const timer = setTimeout(() => {
  stopPromise = stopGroup();
}, timeoutSeconds * 1000);
const exitPromise = new Promise((resolve) => {
  child.on("error", (error) => resolve({ code: null, signal: null, error: String(error) }));
  child.on("close", (code, signal) => {
    closed = true;
    for (const [id, deliver] of [...rpcPending.entries()]) {
      const record = rpcCommands.find((candidate) => candidate.id === id);
      if (record && record.success !== true) {
        record.success = false;
        record.error ??= "child exited before response";
      }
      deliver(null);
    }
    resolve({ code, signal });
  });
});

// RPC: send the prompt as a command, keep stdin open until the expected runs
// settle and the queue is drained, then capture state/entries/stats and EOF.
let rpcState = null;
let rpcStats = null;
let rpcEntries = null;
if (rpcMode) {
  const promptResponse = await rpcRequest({ type: "prompt", message: prompt });
  if (promptResponse === null || promptResponse.success !== true) {
    rpcProblems.push("rpc prompt was not accepted");
  }
  await until(() => settledRuns >= rpcRuns);
  for (let attempt = 0; attempt < 5; attempt += 1) {
    rpcState = await rpcRequest({ type: "get_state" });
    if (rpcState === null || rpcState.success !== true) break;
    const data = rpcState.data ?? {};
    if (data.isStreaming === false && (data.pendingMessageCount ?? 0) === 0) break;
    const settledBefore = settledRuns;
    if (!(await until(() => settledRuns > settledBefore))) break;
  }
  rpcStats = await rpcRequest({ type: "get_session_stats" });
  rpcEntries = await rpcRequest({ type: "get_entries" });
  try {
    child.stdin.end();
  } catch {
    /* stdin already closed */
  }
}

const exit = await exitPromise;
clearTimeout(timer);
if (killTimer) clearTimeout(killTimer);
// The graceful-abort bookkeeping may still be settling when the child closes;
// wait for it so finished.json records the final abort outcome, not a race.
if (stopPromise) await stopPromise;
if (tail.length > 0) fs.writeFileSync(path.join(outDir, "stdout-tail.bin"), tail);
for (const fd of [eventsFd, arrivalFd, stderrFd]) fs.closeSync(fd);

// Opt-in independent task verification runs after the child is gone, and its
// verdict is recorded in its own receipt: a failing check is artifact
// evidence, never a runner problem, so finished.ok stays lifecycle-only.
if (taskCheckSpec !== null) {
  const receipt = runTaskCheck(taskCheckSpec);
  writeExclusive(path.join(outDir, "task-check.json"), JSON.stringify(receipt, null, 2) + "\n");
}

const problems = [];
for (const message of rpcProblems) problems.push(message);
if (rpcMode) {
  if (settledRuns < rpcRuns) problems.push(`rpc recorded ${settledRuns}/${rpcRuns} agent settles`);
  if (rpcState === null) problems.push("rpc get_state had no response");
  else if (rpcState.success !== true) problems.push(`rpc get_state failed: ${rpcState.error ?? "unknown"}`);
  else if (rpcState.data?.isStreaming !== false || (rpcState.data?.pendingMessageCount ?? 0) > 0) {
    problems.push("rpc session was not idle with an empty queue after the expected runs");
  }
}
if (exit.error) problems.push(`failed to spawn ${piBinary}: ${exit.error}`);
if (exit.code !== 0) {
  problems.push(`pi exited with code ${JSON.stringify(exit.code)} signal ${JSON.stringify(exit.signal)}`);
}
if (timedOut) {
  problems.push(
    gracefulAbort.settled
      ? `timed out after ${timeoutSeconds}s; graceful RPC abort completed within ${abortGraceSeconds}s`
      : `timed out after ${timeoutSeconds}s; killed the owned process group`,
  );
}
if (tail.length > 0) {
  problems.push(`stdout ended with ${tail.length} unterminated bytes (preserved in stdout-tail.bin)`);
}

const telemetryPath = path.join(outDir, "telemetry.jsonl");
let telemetryShutdown = false;
if (!fs.existsSync(telemetryPath)) {
  problems.push("telemetry.jsonl missing");
} else {
  for (const [index, line] of fs.readFileSync(telemetryPath, "utf8").split("\n").entries()) {
    if (!line.trim()) continue;
    try {
      const record = JSON.parse(line);
      if (record === null || typeof record !== "object" || Array.isArray(record)) {
        throw new Error("not an object");
      }
      if (record.type === "session_shutdown") telemetryShutdown = true;
      if (record.type === "turn_limit") problems.push("canary turn limit reached");
    } catch {
      problems.push(`malformed telemetry line ${index + 1}`);
    }
  }
}
if (!telemetryShutdown) problems.push("telemetry has no session_shutdown record");

if (rpcMode) {
  fs.writeFileSync(
    path.join(outDir, "rpc.json"),
    JSON.stringify(
      {
        mode: "rpc",
        rpcRuns,
        settlements: settledRuns,
        commands: rpcCommands,
        state: rpcState?.success === true ? rpcState.data : null,
        stats: rpcStats?.success === true ? rpcStats.data : null,
        errors: rpcProblems,
      },
      null,
      2,
    ) + "\n",
    { flag: "wx" },
  );
  fs.writeFileSync(
    path.join(outDir, "rpc-entries.json"),
    JSON.stringify(rpcEntries?.success === true ? rpcEntries.data?.entries ?? [] : null, null, 2) + "\n",
    { flag: "wx" },
  );
}

const ok = problems.length === 0;
writeExclusive(
  path.join(outDir, "finished.json"),
  JSON.stringify(
    {
      ok,
      problems,
      mode: rpcMode ? "rpc" : "json",
      turns,
      rpcRuns: rpcMode ? rpcRuns : null,
      settlements: rpcMode ? settledRuns : null,
      exit,
      timedOut,
      gracefulAbort: rpcMode ? gracefulAbort : null,
      unterminatedTailBytes: tail.length,
      wallMs: Date.now() - startedAt,
      stdoutLines: lineIndex,
      telemetryShutdown,
    },
    null,
    2,
  ) + "\n",
);
console.log(JSON.stringify({ ok, problems, outDir, wallMs: Date.now() - startedAt, stdoutLines: lineIndex }));
process.exitCode = ok ? 0 : 1;
