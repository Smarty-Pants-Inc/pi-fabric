#!/usr/bin/env node
// Maintained verifier for a canary cell produced by bench/prewalk/prewalk-canary-run.mjs.
//
// Reads the recorded evidence (started/finished/rpc/events/telemetry/sessions
// and an optional probe.jsonl) and produces a tri-state check ledger: pass,
// fail or unobserved. An unobserved required check blocks ok exactly like a
// failure, so a run cannot look green just because evidence is missing. The
// verifier never writes into the run directory; --json writes a NEW file and
// refuses to overwrite an existing one.
//
// usage: node bench/prewalk/verify-prewalk-canary.mjs --run <cellDir> \
//        [--json <newFile>] [--report <newFile>] [--dist <dir>] \
//        [--main <provider/model>] [--executor <provider/model>] \
//        [--recovery-marker <text>] [--expect-prewalk <n>] [--request-contract <file>]

import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  assertCompleteRecording,
  assistantTimeline,
  aggregateUsage,
  attributePhases,
  createCheckLedger,
  extractPrewalkStatus,
  parseJsonLines,
  parseRequestContract,
  persistedPrewalkMessages,
  requestContractEvidenceProblems,
  requestContractPayloadProblems,
  taskCheckReceiptProblems,
  telemetryTimeline,
  toolResultUsages,
} from "./lib/prewalk-live-evidence.mjs";

const argv = process.argv.slice(2);
const value = (flag) => {
  const index = argv.indexOf(flag);
  return index >= 0 ? argv[index + 1] : undefined;
};
const fail = (error) => {
  console.error(JSON.stringify({ ok: false, error }));
  process.exit(2);
};

const runArg = value("--run");
if (!runArg) fail("usage: node bench/prewalk/verify-prewalk-canary.mjs --run <cellDir> [--json <newFile>] ...");
const run = path.resolve(runArg);
if (!fs.existsSync(run) || !fs.lstatSync(run).isDirectory()) fail(`--run is not a directory: ${run}`);
const jsonOut = value("--json");
const reportOut = value("--report");
const mainModel = value("--main");
const executorModel = value("--executor");
const recoveryMarker = value("--recovery-marker");
const requestContractFile = value("--request-contract");
const taskCheckReport = value("--task-check-report");
const scopeReport = value("--scope-report");
const workDirFlag = value("--work-dir");
const expectPrewalk = value("--expect-prewalk") === undefined ? null : Number(value("--expect-prewalk"));
const distDir = path.resolve(value("--dist") ?? path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "dist"));
if (expectPrewalk !== null && (!Number.isInteger(expectPrewalk) || expectPrewalk < 0)) {
  fail("--expect-prewalk must be a non-negative integer");
}
// --main alone enables recovery model attribution (recovery-reply); the
// in-place roundtrip check additionally needs --executor, which is only
// meaningful together with --main.
if (executorModel !== undefined && mainModel === undefined) fail("--executor requires --main");

const readJson = (name, required = true) => {
  const file = path.join(run, name);
  if (!fs.existsSync(file)) {
    if (required) fail(`${name} is missing`);
    return null;
  }
  return JSON.parse(fs.readFileSync(file, "utf8"));
};
const readJsonl = (name, required = true) => {
  const file = path.join(run, name);
  if (!fs.existsSync(file)) {
    if (required) fail(`${name} is missing`);
    return [];
  }
  const text = fs.readFileSync(file, "utf8");
  if (!text.trim()) {
    if (required) fail(`${name} is empty`);
    return [];
  }
  return parseJsonLines(text, name);
};
// Artifact re-verification needs the cell's work directory: prefer the
// receipt's own absolute cwd, then the runner's started.json provenance.
const workDirOf = (receipt) => {
  for (const candidate of [receipt?.cwd, started?.cwd, workDirFlag]) {
    if (typeof candidate === "string" && path.isAbsolute(candidate)) return path.resolve(candidate);
  }
  return null;
};
const artifactSha256Of = (receipt, workDir) => {
  const relative = receipt?.artifact?.path;
  if (typeof relative !== "string" || workDir === null) return undefined;
  const resolved = path.resolve(workDir, relative);
  if (!resolved.startsWith(workDir + path.sep)) return undefined;
  if (!fs.existsSync(resolved) || !fs.lstatSync(resolved).isFile()) return null;
  return createHash("sha256").update(fs.readFileSync(resolved)).digest("hex");
};
const modelKeyOf = (message) =>
  typeof message?.provider === "string" && typeof message?.model === "string"
    ? `${message.provider}/${message.model}`
    : null;

const started = readJson("started.json");
const finished = readJson("finished.json");
const mode = started.mode === "rpc" ? "rpc" : "json";
const events = readJsonl("events.jsonl");
const telemetry = readJsonl("telemetry.jsonl");
const rpc = readJson("rpc.json", false);
const probeFile = path.join(run, "probe.jsonl");
const probe = fs.existsSync(probeFile) && fs.statSync(probeFile).size > 0
  ? parseJsonLines(fs.readFileSync(probeFile, "utf8"), "probe.jsonl")
  : null;

const sessionsDir = path.join(run, "sessions");
const sessionEntries = [];
if (fs.existsSync(sessionsDir) && fs.lstatSync(sessionsDir).isDirectory()) {
  for (const name of fs.readdirSync(sessionsDir).filter((candidate) => candidate.endsWith(".jsonl"))) {
    // An empty session file is a legitimate recording (no persisted entries);
    // parseJsonLines rejects empty input by contract, so skip it here.
    const text = fs.readFileSync(path.join(sessionsDir, name), "utf8");
    if (!text.trim()) continue;
    sessionEntries.push(...parseJsonLines(text, name));
  }
}

// Axis by check name: lifecycle (runtime and handoff), scope (write
// compliance) and artifact (independent test evidence) stay separate verdicts,
// so a green lifecycle can never stand in for passing tests.
const CHECK_AXES = {
  "task-verification": "artifact",
  "scope-no-writes-after-marker": "scope",
  "recovery-no-writes-after-cancel": "scope",
  "recovery-work-preserved": "scope",
};
const rawLedger = createCheckLedger();
const ledger = {
  add: (name, status, detail) => rawLedger.add(name, status, detail, CHECK_AXES[name] ?? "lifecycle"),
  checks: rawLedger.checks,
  axes: rawLedger.axes,
  ok: rawLedger.ok,
  toJSON: rawLedger.toJSON,
};

// 1. Lifecycle completeness. A recovery canary aborts the executor on
// purpose; every other run must fail on a non-complete assistant.
try {
  assertCompleteRecording(events, telemetry, {
    requireSessionHeader: mode !== "rpc",
    ...(mode === "rpc" && typeof rpc?.state?.sessionId === "string" ? { sessionId: rpc.state.sessionId } : {}),
    ...(recoveryMarker !== undefined ? { allowAborted: true } : {}),
  });
  ledger.add("recording-complete", "pass");
} catch (error) {
  ledger.add("recording-complete", "fail", String(error?.message ?? error));
}

// 2. The runner's own problems are part of the verdict.
if (finished.ok === true && Array.isArray(finished.problems) && finished.problems.length === 0) {
  ledger.add("finished-ok", "pass");
} else {
  ledger.add("finished-ok", "fail", { ok: finished.ok ?? null, problems: finished.problems ?? null });
}

// 3. Structured loaded-versus-disk identity from the prewalk.status tool
// result. The child may return the status text on its own or nested inside a
// batched Fabric envelope (descriptions list, a `status:` block, a helper
// appendix); extractPrewalkStatus recognizes both and treats quoted or
// descriptive text as no evidence. Every observation in the recording is
// evaluated, so a later stale or contradictory sample cannot hide behind an
// earlier clean one.
const statusCandidates = [];
for (const event of events) {
  const message = event.message;
  if (event.type !== "message_end" || message?.role !== "toolResult") continue;
  const parts = Array.isArray(message.content) ? message.content : [];
  for (const part of parts) {
    if (!part || typeof part.text !== "string") continue;
    const parsed = extractPrewalkStatus(part.text);
    if (parsed === null) continue;
    statusCandidates.push({ toolName: message.toolName ?? null, toolCallId: message.toolCallId ?? null, parsed });
    break;
  }
}
if (statusCandidates.length === 0) {
  ledger.add("runtime-identity", "unobserved", "no prewalk.status tool result in the recording");
} else {
  const problems = [];
  const expected = { entry: "index.js", lazyRuntime: "fabric-runtime-state.js" };
  for (const { toolCallId, parsed } of statusCandidates) {
    for (const key of ["entry", "lazyRuntime"]) {
      const label = `${toolCallId ?? "?"}:${key}`;
      const artifact = parsed.runtime[key];
      if (!artifact || typeof artifact !== "object" || Array.isArray(artifact)) {
        problems.push(`${label} missing`);
        continue;
      }
      const { path: artifactPath, loadedSha256, diskSha256, stale } = artifact;
      if (typeof artifactPath !== "string" || typeof loadedSha256 !== "string" || typeof diskSha256 !== "string") {
        problems.push(`${label} fields missing`);
        continue;
      }
      if (!/^[0-9a-f]{64}$/.test(loadedSha256) || !/^[0-9a-f]{64}$/.test(diskSha256)) problems.push(`${label} sha256 malformed`);
      if (artifactPath !== path.join(distDir, expected[key])) problems.push(`${label} path outside --dist: ${artifactPath}`);
      if (loadedSha256 !== diskSha256) problems.push(`${label} loaded != disk`);
      // The JSON envelope carries a boolean; indented text carries "false".
      if (!(stale === false || stale === "false")) problems.push(`${label} stale=${String(stale)}`);
    }
  }
  const first = statusCandidates[0];
  ledger.add("runtime-identity", problems.length === 0 ? "pass" : "fail", {
    toolName: first.toolName,
    observations: statusCandidates.length,
    problems,
    entry: first.parsed.runtime.entry ?? null,
    lazyRuntime: first.parsed.runtime.lazyRuntime ?? null,
  });
}

// 4. RPC recordings prove the same session: get_state's sessionId must match
// the telemetry session_start identity.
if (mode === "rpc") {
  const sessionStart = telemetry.find((record) => record.type === "session_start");
  if (!rpc || typeof rpc.state?.sessionId !== "string" || !sessionStart) {
    ledger.add("session-identity", "unobserved", "rpc state or telemetry session_start missing");
  } else if (rpc.state.sessionId !== sessionStart.sessionId) {
    ledger.add("session-identity", "fail", { rpc: rpc.state.sessionId, telemetry: sessionStart.sessionId },
    );
  } else {
    ledger.add("session-identity", "pass", { sessionId: rpc.state.sessionId });
  }
}

// 5. Exactly one Main->executor->Main roundtrip when both models are named.
if (mainModel !== undefined && executorModel !== undefined) {
  const selects = telemetryTimeline(telemetry).modelSelects;
  const [first, second] = selects;
  const ok =
    selects.length === 2 &&
    first?.previous === mainModel &&
    first?.model === executorModel &&
    second?.previous === executorModel &&
    second?.model === mainModel;
  ledger.add("in-place-roundtrip", ok ? "pass" : "fail", selects);
}

// 6. Persisted prewalk custom_message count against an explicit expectation.
if (expectPrewalk !== null) {
  const persisted = persistedPrewalkMessages(sessionEntries);
  ledger.add(
    "persisted-prewalk",
    persisted.length === expectPrewalk ? "pass" : "fail",
    persisted.map((message) => ({ id: message.id ?? null, customType: message.customType })),
  );
}

// 7. Recovery checks. The marker is the frozen read-only recovery text.
if (recoveryMarker !== undefined) {
  const abortIndex = events.findIndex(
    (event) =>
      event.type === "message_end" &&
      event.message?.role === "assistant" &&
      event.message.stopReason === "aborted",
  );
  ledger.add(
    "recovery-abort-recorded",
    abortIndex >= 0 ? "pass" : "fail",
    abortIndex >= 0 ? { index: abortIndex, model: modelKeyOf(events[abortIndex].message) } : null,
  );

  const textOf = (message) =>
    Array.isArray(message?.content)
      ? message.content.filter((part) => part && typeof part.text === "string").map((part) => part.text).join("\n")
      : typeof message?.content === "string"
        ? message.content
        : "";
  const recoveryIndexes = [];
  events.forEach((event, index) => {
    if (event.type === "message_end" && event.message?.role === "user" && textOf(event.message).includes(recoveryMarker)) {
      recoveryIndexes.push(index);
    }
  });
  const deliveredOnce = recoveryIndexes.length === 1 && abortIndex >= 0 && recoveryIndexes[0] > abortIndex;
  ledger.add("recovery-delivered-once", deliveredOnce ? "pass" : "fail", {
    count: recoveryIndexes.length,
    indexes: recoveryIndexes,
  });

  const recoveryIndex = recoveryIndexes.length === 1 ? recoveryIndexes[0] : null;
  if (recoveryIndex === null) {
    ledger.add("recovery-tools", "unobserved", "recovery message not uniquely recorded");
    ledger.add("recovery-reply", "unobserved", "recovery message not uniquely recorded");
  } else {
    const pendingCalls = new Map();
    let completedTools = 0;
    for (let index = recoveryIndex + 1; index < events.length; index += 1) {
      const message = events[index].message;
      if (events[index].type !== "message_end") continue;
      if (message?.role === "assistant") {
        const parts = Array.isArray(message.content) ? message.content : [];
        for (const part of parts) {
          if (part?.type === "toolCall" && typeof part.id === "string") pendingCalls.set(part.id, part.name);
        }
      } else if (message?.role === "toolResult" && typeof message.toolCallId === "string" && pendingCalls.has(message.toolCallId)) {
        pendingCalls.delete(message.toolCallId);
        if (message.isError !== true) completedTools += 1;
      }
    }
    ledger.add("recovery-tools", completedTools >= 1 ? "pass" : "fail", { completedTools });

    const reply = events
      .slice(recoveryIndex + 1)
      .find(
        (event) =>
          event.type === "message_end" &&
          event.message?.role === "assistant" &&
          event.message.stopReason === "stop" &&
          modelKeyOf(event.message) === mainModel,
      );
    ledger.add("recovery-reply", reply ? "pass" : "fail", reply ? { model: modelKeyOf(reply.message) } : null);
  }

  // No source mutation after the abort. write/edit calls are visible in the
  // event stream; opaque tools cannot be judged here and are covered by the
  // probe snapshot check below.
  const writeTools = new Set(["write", "edit"]);
  const writesAfter = [];
  if (abortIndex >= 0) {
    for (const event of events.slice(abortIndex + 1)) {
      if (event.type === "tool_execution_start" && writeTools.has(event.toolName)) writesAfter.push(event.toolName);
      if (event.type === "message_end" && event.message?.role === "assistant") {
        const parts = Array.isArray(event.message.content) ? event.message.content : [];
        for (const part of parts) {
          if (part?.type === "toolCall" && writeTools.has(part.name)) writesAfter.push(part.name);
        }
      }
    }
  }
  ledger.add(
    "recovery-no-writes-after-cancel",
    abortIndex < 0 ? "unobserved" : writesAfter.length === 0 ? "pass" : "fail",
    writesAfter,
  );

  // Preserved partial work needs the probe's source snapshots; without them
  // the check stays unobserved rather than guessed.
  if (probe === null) {
    ledger.add("recovery-work-preserved", "unobserved", "probe.jsonl not present");
  } else {
    const snapshots = probe.filter((record) => record.type === "snapshot" && typeof record.label === "string");
    const cancelSnapshot = [...snapshots].reverse().find((record) => record.label === "before_cancel");
    const shutdownSnapshot = [...snapshots].reverse().find((record) => record.label === "shutdown");
    if (!cancelSnapshot?.sourceHash || !shutdownSnapshot?.sourceHash) {
      ledger.add("recovery-work-preserved", "unobserved", "before_cancel/shutdown snapshots missing");
    } else {
      ledger.add(
        "recovery-work-preserved",
        cancelSnapshot.sourceHash === shutdownSnapshot.sourceHash ? "pass" : "fail",
        { cancel: cancelSnapshot.sourceHash, shutdown: shutdownSnapshot.sourceHash },
      );
    }
  }
}

// 8. Opt-in request-contract evidence. A contract is declared by
// --request-contract or the runner's started.json provenance; without one no
// check is recorded, so cells that never opted in are unaffected. Missing
// records are unobserved (blocking), malformed or mismatched records fail, and
// payload semantics need --executor to select executor requests.
const declaredContractPath =
  requestContractFile ??
  (typeof started.requestContract?.path === "string" ? started.requestContract.path : undefined);
let requestContractSummary = null;
if (declaredContractPath !== undefined) {
  let contract = null;
  let contractSha256 = null;
  let contractError = null;
  try {
    const resolved = path.resolve(declaredContractPath);
    const text = fs.readFileSync(resolved, "utf8");
    contract = parseRequestContract(text, resolved);
    contractSha256 = createHash("sha256").update(text).digest("hex");
    if (typeof started.requestContract?.sha256 === "string" && started.requestContract.sha256 !== contractSha256) {
      contractError = `contract sha256 differs from started.json provenance (${started.requestContract.sha256} != ${contractSha256})`;
    }
  } catch (error) {
    contractError = String(error?.message ?? error);
  }
  const records = telemetry.filter((record) => record.type === "request_context");
  requestContractSummary = { path: path.resolve(declaredContractPath), sha256: contractSha256, records: records.length };
  if (contractError !== null) {
    ledger.add("request-contract-evidence", "fail", contractError);
    ledger.add("request-contract-payload", "unobserved", contractError);
  } else if (records.length === 0) {
    ledger.add("request-contract-evidence", "unobserved", "no request_context records in telemetry");
    ledger.add("request-contract-payload", "unobserved", "no request_context records in telemetry");
  } else {
    const evidenceProblems = requestContractEvidenceProblems(records, contract, contractSha256);
    ledger.add(
      "request-contract-evidence",
      evidenceProblems.length === 0 ? "pass" : "fail",
      evidenceProblems.length === 0 ? { requests: records.length } : evidenceProblems,
    );
    if (executorModel === undefined) {
      ledger.add("request-contract-payload", "unobserved", "--executor is required for payload semantics");
    } else {
      const payloadProblems = requestContractPayloadProblems(records, contract, executorModel);
      ledger.add(
        "request-contract-payload",
        payloadProblems.length === 0 ? "pass" : "fail",
        payloadProblems.length === 0 ? { requests: records.length } : payloadProblems,
      );
    }
  }
}

// Informational accounting: deterministic Fabric compaction is LLM-free, so
// absent usage must never be reported as a missing paid call.
const compaction = sessionEntries
  .filter((entry) => entry.type === "compaction")
  .map((entry) => ({
    compactor: entry.details?.compactor ?? null,
    version: entry.details?.version ?? null,
    fromHook: entry.fromHook ?? null,
    usageRecorded: entry.usage !== undefined && entry.usage !== null,
  }));

// 9. Opt-in artifact verdict. A lifecycle-clean cell proves nothing about the
// artifact it left behind, so failing or unverifiable tests are recorded on
// their own axis: the receipt's counts and its content hash binding decide,
// and a missing receipt stays unobserved rather than assumed green.
let taskCheckSummary = null;
if (taskCheckReport !== undefined) {
  const receiptPath = path.resolve(taskCheckReport);
  let receipt = null;
  let reason = null;
  try {
    receipt = JSON.parse(fs.readFileSync(receiptPath, "utf8"));
  } catch (error) {
    reason = String(error?.message ?? error);
  }
  if (receipt === null || receipt === undefined) {
    ledger.add("task-verification", "unobserved", reason ?? "task-check receipt missing");
    taskCheckSummary = { path: receiptPath, sha256: null };
  } else {
    const workDir = workDirOf(receipt);
    taskCheckSummary = {
      path: receiptPath,
      sha256: createHash("sha256").update(fs.readFileSync(receiptPath)).digest("hex"),
      testFile: receipt.testFile ?? null,
      counts: receipt.counts ?? null,
      artifact: receipt.artifact ?? null,
    };
    if (workDir === null) {
      // The artifact's current content cannot be re-verified, so a receipt that
      // only reports its own hashes must not add up to a pass.
      ledger.add(
        "task-verification",
        "unobserved",
        "work directory unresolved; artifact content could not be re-verified",
      );
    } else {
      const problems = taskCheckReceiptProblems(receipt, { artifactSha256: artifactSha256Of(receipt, workDir) });
      ledger.add(
        "task-verification",
        problems.length === 0 ? "pass" : "fail",
        problems.length === 0
          ? { testFile: receipt.testFile ?? null, counts: receipt.counts ?? null, artifact: receipt.artifact ?? null }
          : problems,
      );
    }
  }
}

// 10. Opt-in write-scope evidence: the recorder reports the paths changed
// after the scope marker. Compliance is its own axis and never substitutes for
// artifact quality. A missing or unreadable report is unobserved, which blocks
// ok exactly like a failure.
let scopeSummary = null;
if (scopeReport !== undefined) {
  const scopePath = path.resolve(scopeReport);
  try {
    const report = JSON.parse(fs.readFileSync(scopePath, "utf8"));
    const writes = Array.isArray(report?.writesAfter) ? report.writesAfter : null;
    scopeSummary = { path: scopePath, marker: report?.marker ?? null, writesAfter: writes };
    if (typeof report?.marker !== "string" || writes === null) {
      ledger.add("scope-no-writes-after-marker", "unobserved", "scope report lacks marker or writesAfter");
    } else {
      const clean = report.ok === true && writes.length === 0;
      ledger.add("scope-no-writes-after-marker", clean ? "pass" : "fail", {
        marker: report.marker,
        writesAfter: writes,
      });
    }
  } catch (error) {
    scopeSummary = { path: scopePath, marker: null, writesAfter: null };
    ledger.add("scope-no-writes-after-marker", "unobserved", `scope report unreadable: ${String(error?.message ?? error)}`);
  }
}

// Informational metrics: per-model usage, phase timing and observed message
// failures derived from the same recorded evidence the checks use. Never a
// check: incomplete usage stays unavailable, never zero, and provider-internal
// retries are not observable through recorded message_end events.
const metrics = (() => {
  try {
    const timeline = assistantTimeline(events);
    const perModel = aggregateUsage(timeline, toolResultUsages(events));
    const phases = attributePhases({ timeline, telemetry: telemetryTimeline(telemetry) });
    const reasoningByModel = {};
    for (const item of timeline) {
      const reasoning = (item.usage ?? {}).reasoning;
      if (item.model === null || typeof reasoning !== "number" || !Number.isFinite(reasoning)) continue;
      reasoningByModel[item.model] = (reasoningByModel[item.model] ?? 0) + reasoning;
    }
    const assistantFailures = events.filter(
      (event) =>
        event.type === "message_end" &&
        event.message?.role === "assistant" &&
        ["error", "aborted"].includes(event.message.stopReason),
    ).length;
    const toolResultErrors = events.filter(
      (event) =>
        event.type === "message_end" &&
        event.message?.role === "toolResult" &&
        event.message.isError === true,
    ).length;
    return {
      available: true,
      wallMs: typeof finished.wallMs === "number" ? finished.wallMs : null,
      assistantCount: timeline.length,
      perModel,
      phases,
      reasoningByModel: Object.keys(reasoningByModel).length > 0 ? reasoningByModel : null,
      observedFailures: {
        assistantErrorOrAborted: assistantFailures,
        toolResultErrors,
        provenance: "recorded message_end stop reasons and toolResult isError flags; provider-internal retries are not observable",
      },
      caveats: [
        "Usage totals come from recorded message_end events; failed attempts without usage are excluded and provider-internal retries are not visible.",
        "Phase spans use assistant message timestamps and extension model_select observations, not network request durations.",
        "Reasoning is a per-model breakdown only and is never added into usage totals.",
      ],
    };
  } catch (error) {
    return { available: false, reason: String(error?.message ?? error) };
  }
})();

const report = {
  schemaVersion: 1,
  generatedAt: new Date().toISOString(),
  run,
  metrics,
  ...ledger.toJSON(),
  info: {
    mode,
    finished: { ok: finished.ok ?? null, wallMs: finished.wallMs ?? null },
    events: events.length,
    settlements: typeof finished.settlements === "number" ? finished.settlements : null,
    compaction,
    requestContract: requestContractSummary,
    taskCheck: taskCheckSummary,
    scope: scopeSummary,
  },
};
// One analysis drives both outputs: the human report renders the same JSON
// verdict, never a second opinion.
const humanReportText = (rendered) => {
  const lines = [
    "# Prewalk canary verification",
    "",
    `run: ${rendered.run}`,
    `generatedAt: ${rendered.generatedAt}`,
    `ok: ${rendered.ok}`,
    "",
    "## Checks",
  ];
  for (const check of rendered.checks) {
    const detail = check.detail === null || check.detail === undefined ? "" : ` — ${JSON.stringify(check.detail)}`;
    lines.push(`- ${check.status.toUpperCase()} ${check.name}${detail}`);
  }
  lines.push("", "## Axes");
  for (const [axis, entries] of Object.entries(rendered.axes ?? {})) {
    lines.push(`- ${axis}: ${entries.map((entry) => `${entry.name}=${entry.status}`).join(", ")}`);
  }
  lines.push("", "## Metrics");
  const m = rendered.metrics;
  if (!m.available) {
    lines.push(`- unavailable: ${m.reason}`);
  } else {
    lines.push(`- wallMs: ${m.wallMs ?? "unrecorded"}`);
    lines.push(`- assistant messages: ${m.assistantCount}`);
    for (const [model, usage] of Object.entries(m.perModel)) {
      lines.push(`- ${model}: ${usage.requests} request(s), input ${usage.input}, cacheRead ${usage.cacheRead}, output ${usage.output}, total ${usage.totalTokens} tokens, est $${usage.recordedCostEstimateUsd}`);
    }
    if (m.reasoningByModel !== null) {
      for (const [model, reasoning] of Object.entries(m.reasoningByModel)) {
        lines.push(`- reasoning breakdown ${model}: ${reasoning} (never added into totals)`);
      }
    }
    const phases = m.phases;
    lines.push(`- phases: handoffAt ${phases.handoffAt ?? "unobserved"}, returnAt ${phases.returnAt ?? "unobserved"}, preHandoffMainMs ${phases.preHandoffMainMs ?? "unobserved"}, executorIntervalMs ${phases.executorIntervalMs ?? "unobserved"}, returnMs ${phases.returnMs ?? "unobserved"}`);
    lines.push(`- observed failures: ${m.observedFailures.assistantErrorOrAborted} assistant error/aborted, ${m.observedFailures.toolResultErrors} tool result errors`);
    for (const caveat of m.caveats) lines.push(`- caveat: ${caveat}`);
  }
  lines.push("");
  return lines.join("\n");
};

const serialized = JSON.stringify(report, null, 2) + "\n";
if (jsonOut !== undefined) {
  try {
    fs.writeFileSync(path.resolve(jsonOut), serialized, { flag: "wx" });
  } catch (error) {
    fail(`--json refuses to overwrite or write an existing file: ${String(error?.message ?? error)}`);
  }
}
if (reportOut !== undefined) {
  try {
    fs.writeFileSync(path.resolve(reportOut), humanReportText(report), { flag: "wx" });
  } catch (error) {
    fail(`--report refuses to overwrite or write an existing file: ${String(error?.message ?? error)}`);
  }
}
process.stdout.write(serialized);
process.exitCode = report.ok ? 0 : 1;
