// Conservative classification, accounting and resume planning for native SWE
// benchmark evidence. Raw grader reports are never rewritten here: these pure
// helpers derive verdicts that callers record next to the input hashes.
//
// Guardrails:
// * a no-op control is acceptable only with proof the test command ran and an
//   expected compile/import failure of code that the gold patch introduces;
//   empty results, missing tools and broken environments stay rejected;
// * gold controls stay strict (valid and fully resolved);
// * a candidate failure is attributed to the model patch only when preflight
//   controls passed and the test command ran; otherwise it is unobserved
//   infrastructure evidence, never silently dropped from comparisons;
// * HTTP response headers never settle usage: unsettled requests hold a
//   conservative per-model reserve and keep the run uncertain.

const NUMBER = (value) => typeof value === "number" && Number.isFinite(value);
const TEXT = (value) => (typeof value === "string" ? value : "");

const NOOP_FAILURE_PATTERNS = [
  /undefined:/, // Go: symbol implemented by the gold patch
  /\[build failed\]/, // Go test build failure
  /ImportError/,
  /cannot import name/,
  /ModuleNotFoundError/,
  /NameError:/,
];
const CANDIDATE_FAILURE_PATTERNS = [...NOOP_FAILURE_PATTERNS, /SyntaxError/, /IndentationError/];
const ENVIRONMENT_FAILURE_PATTERNS = [
  /command not found/,
  /: not found\b/,
  /Cannot read properties of undefined/,
  /No such file or directory/,
  /ENOENT/,
  /out of memory/i,
  /\bOOM\b/,
  /Killed/,
  /SIGSEGV/,
  /segfault/i,
];

function failureEvidence(text, patterns) {
  if (!text.trim()) return { ok: false, reason: "no failure output recorded" };
  for (const pattern of ENVIRONMENT_FAILURE_PATTERNS) {
    if (pattern.test(text)) return { ok: false, reason: `environment/tooling failure marker: ${pattern}` };
  }
  for (const pattern of patterns) if (pattern.test(text)) return { ok: true };
  return { ok: false, reason: "no expected compile/import failure evidence" };
}

function goldVerdict(arm) {
  if (!arm) return { status: "fail", reason: "missing gold control" };
  const { result, execution } = arm;
  if (result?.error != null) return { status: "fail", reason: `gold grader error: ${result.error}` };
  if (result?.valid !== true || result?.resolved !== true) {
    return { status: "fail", reason: "gold control did not resolve its required tests" };
  }
  if (execution?.setupOk === false) return { status: "fail", reason: "gold setup failed" };
  if (execution?.testStarted === false) return { status: "fail", reason: "gold test command did not run" };
  return { status: "pass", reason: "gold resolved all required tests" };
}

function noopVerdict(arm, goldStatus) {
  if (!arm) return { status: "fail", acceptable: false, reason: "missing no-op control" };
  const { result, execution } = arm;
  const status = result?.resolved === true ? "pass" : "fail";
  if (goldStatus !== "pass") return { status, acceptable: false, reason: "gold control did not pass" };
  if (status === "pass") return { status, acceptable: false, reason: "no-op control resolved tests" };
  if (result?.error != null) return { status, acceptable: false, reason: `no-op grader error: ${result.error}` };
  if (execution?.setupOk === false) return { status, acceptable: false, reason: "no-op setup failed" };
  if (execution?.testStarted === false) return { status, acceptable: false, reason: "no-op test command did not run" };
  if (NUMBER(result?.parsedTests) && result.parsedTests > 0) {
    if (NUMBER(result.requiredTestsPassed) && result.requiredTestsPassed < result.requiredTests) {
      return { status, acceptable: true, reason: "tests ran and required tests failed without the gold patch" };
    }
    return { status, acceptable: false, reason: "tests ran but required tests unexpectedly passed" };
  }
  const evidence = failureEvidence(
    `${TEXT(execution?.stdout)}\n${TEXT(execution?.stderr)}`,
    NOOP_FAILURE_PATTERNS,
  );
  return evidence.ok
    ? { status, acceptable: true, reason: "expected compile/import failure without the gold patch" }
    : { status, acceptable: false, reason: evidence.reason };
}

export function classifyControls(arms) {
  if (!arms || typeof arms !== "object") throw new Error("classifyControls requires {noop, gold} arms");
  const gold = goldVerdict(arms.gold);
  const noop = noopVerdict(arms.noop, gold.status);
  return { ok: gold.status === "pass" && noop.acceptable, noop, gold };
}

export function classifyCandidate({ result, controlsPassed, execution }) {
  if (controlsPassed !== true) {
    return { status: "unobserved", validComparison: false, reason: "preflight controls did not pass" };
  }
  if (!execution || execution.testStarted === false) {
    return { status: "unobserved", validComparison: false, reason: "test command did not run" };
  }
  if (!result) return { status: "unobserved", validComparison: false, reason: "no grade result recorded" };
  if (result.error != null) {
    return { status: "unobserved", validComparison: false, reason: `grader error: ${result.error}` };
  }
  if (result.resolved === true) {
    return { status: "pass", validComparison: true, reason: "candidate resolved all required tests" };
  }
  if (NUMBER(result.parsedTests) && result.parsedTests > 0) {
    return { status: "fail", validComparison: true, reason: "candidate tests ran and required tests failed" };
  }
  const evidence = failureEvidence(
    `${TEXT(execution.stdout)}\n${TEXT(execution.stderr)}`,
    CANDIDATE_FAILURE_PATTERNS,
  );
  if (evidence.ok) {
    return { status: "fail", validComparison: true, reason: "candidate build/collection failure attributed to the patch" };
  }
  return { status: "unobserved", validComparison: false, reason: evidence.reason };
}

export function analyzeRequestLifecycle(events, limits) {
  if (!Array.isArray(events)) throw new Error("events must be an array");
  const rates = limits?.maximumRequestUsd;
  if (!rates || typeof rates !== "object") throw new Error("maximumRequestUsd limits required");
  for (const [model, cap] of Object.entries(rates)) {
    if (!NUMBER(cap) || cap < 0) throw new Error(`invalid usage cap for ${model}`);
  }
  const pending = [];
  const seen = new Set();
  // Remove the latest pending request for one model; a model-less identity can
  // never match, because a provider_request requires a model string.
  const settlePending = (model) => {
    for (let i = pending.length - 1; i >= 0; i -= 1) {
      if (pending[i].model === model) {
        pending.splice(i, 1);
        return;
      }
    }
  };
  let knownUsd = 0;
  let uncertain = false;
  for (const event of events) {
    if (!event || typeof event !== "object") throw new Error("malformed lifecycle event");
    if (event.type === "provider_request") {
      const number = event.number;
      if (seen.has(number)) throw new Error(`duplicate provider request identity ${JSON.stringify(number)}`);
      seen.add(number);
      const model = typeof event.model === "string" ? event.model : null;
      if (!model) throw new Error("provider request without model identity");
      pending.push({ number, model });
    } else if (event.type === "assistant_end") {
      const usage = event.usage?.cost?.total;
      if (NUMBER(usage) && usage >= 0) knownUsd += usage;
      const stopReason = typeof event.stopReason === "string" ? event.stopReason : null;
      if (stopReason === "error" || stopReason === "aborted") {
        uncertain = true;
      } else if (NUMBER(usage)) {
        settlePending(typeof event.model === "string" ? event.model : null);
      }
    } else if (event.type === "compaction") {
      const usage = event.usage?.cost?.total;
      if (NUMBER(usage) && usage >= 0) {
        knownUsd += usage;
        // Settle the request this compaction actually finished. A model identity
        // removes only that request; without one the event is unambiguous only
        // while a single request is open, so every hold is kept otherwise.
        const model = typeof event.model === "string" ? event.model : null;
        if (model !== null) settlePending(model);
        else if (pending.length === 1) pending.pop();
      }
    }
  }
  let heldUsd = 0;
  for (const request of pending) {
    const cap = rates[request.model];
    if (!NUMBER(cap)) throw new Error(`no conservative usage cap for model ${request.model}`);
    heldUsd += cap;
  }
  return { uncertain: uncertain || pending.length > 0, unsettled: pending, heldUsd, knownUsd };
}

export function summarizeAttempts(rows) {
  if (!Array.isArray(rows)) throw new Error("summarizeAttempts requires an array of rows");
  const attempted = rows.filter((row) => NUMBER(row?.modelRequests) && row.modelRequests > 0);
  const modesByIndex = new Map();
  for (const row of attempted) {
    const key = row.index ?? row.id;
    const modes = modesByIndex.get(key) ?? new Set();
    if (typeof row.mode === "string") modes.add(row.mode);
    modesByIndex.set(key, modes);
  }
  let executedPairs = 0;
  for (const modes of modesByIndex.values()) if (modes.size >= 2) executedPairs += 1;
  return {
    processed: rows.length,
    attempted: attempted.length,
    skipped: rows.length - attempted.length,
    graded: attempted.filter((row) => row.validGrade === true).length,
    resolved: attempted.filter((row) => row.resolved === true).length,
    executedPairs,
  };
}

export function planResume(schedule, checkpoints) {
  if (!Array.isArray(schedule)) throw new Error("planResume requires a schedule array");
  if (!Array.isArray(checkpoints)) throw new Error("planResume requires a checkpoints array");
  const byId = new Map(checkpoints.map((checkpoint) => [checkpoint.id, checkpoint]));
  const runnable = [];
  const needsRecovery = [];
  const completed = [];
  for (const entry of schedule) {
    const checkpoint = byId.get(entry.id);
    if (!checkpoint) runnable.push(entry);
    else if (checkpoint.phase === "finished") completed.push(entry);
    else needsRecovery.push(entry);
  }
  return { runnable, needsRecovery, completed };
}

export function comparePairs(rows) {
  const byIndex = new Map();
  for (const row of rows) {
    const key = row?.index ?? row?.id;
    const list = byIndex.get(key) ?? [];
    list.push(row);
    byIndex.set(key, list);
  }
  const pairs = [...byIndex.entries()].map(([index, arms]) => {
    const astra = arms.find((arm) => arm.mode === "astra") ?? null;
    const prewalk = arms.find((arm) => arm.mode === "prewalk") ?? null;
    const bothComparable =
      astra?.verdict?.validComparison === true && prewalk?.verdict?.validComparison === true;
    const matchedTree =
      astra?.baselineTree && prewalk?.baselineTree ? astra.baselineTree === prewalk.baselineTree : null;
    // Only two recorded, identical trees make a pair comparable; an unrecorded
    // tree is an unknown, not an implicit match.
    const comparable = bothComparable && matchedTree === true;
    const resolved = (arm) => arm?.verdict?.status === "pass";
    let status = "incomplete-or-unobserved";
    if (comparable) {
      const a = resolved(astra);
      const p = resolved(prewalk);
      status =
        a && p ? "both-resolved" : !a && !p ? "both-unresolved" : a ? "astra-only-resolved" : "prewalk-only-resolved";
    }
    return { index, astra, prewalk, comparable, matchedTree, status };
  });
  const comparablePairs = pairs.filter((pair) => pair.comparable);
  const bothSuccess = comparablePairs.filter(
    (pair) => pair.status === "both-resolved" && !pair.astra.timedOut && !pair.prewalk.timedOut,
  );
  const mean = (mode) =>
    bothSuccess.length
      ? bothSuccess.reduce((total, pair) => total + (pair[mode]?.solverDurationSeconds ?? 0), 0) / bothSuccess.length
      : null;
  const cost = (mode, high) =>
    comparablePairs.reduce(
      (total, pair) => total + (pair[mode]?.usageUsd ?? 0) + (high ? (pair[mode]?.usageHoldUsd ?? 0) : 0),
      0,
    );
  return {
    pairs,
    comparablePairs: comparablePairs.length,
    bothResolved: comparablePairs.filter((pair) => pair.status === "both-resolved").length,
    astraOnlyResolved: comparablePairs.filter((pair) => pair.status === "astra-only-resolved").length,
    prewalkOnlyResolved: comparablePairs.filter((pair) => pair.status === "prewalk-only-resolved").length,
    bothUnresolved: comparablePairs.filter((pair) => pair.status === "both-unresolved").length,
    costIntervalUsd: { astra: [cost("astra", false), cost("astra", true)], prewalk: [cost("prewalk", false), cost("prewalk", true)] },
    meanSuccessfulSolverSeconds: { astra: mean("astra"), prewalk: mean("prewalk") },
    note: "Intent-to-treat pairing: invalid grading, timeouts and unsettled usage stay visible; cost intervals include conservative holds.",
  };
}
