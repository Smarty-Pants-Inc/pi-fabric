#!/usr/bin/env node
// Recorded evidence only. No production imports, subprocesses, or host operations.
import { readFile } from "node:fs/promises";
import { isAbsolute, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const LOCK_CLASSES = Object.freeze(["main", "task", "stop"]);
const workloads = ["conformance", "faults", "canary"];
const requireThat = (test, message) => { if (!test) throw new Error(message); };
const count = (n) => Number.isSafeInteger(n) && n >= 0;
const text = (s) => typeof s === "string" && s.length > 0;
const identity = (p) => JSON.stringify([p.machine, p.pid, p.pidStartTime, p.bootId]);
const checkIdentity = (p) => requireThat(p && text(p.machine) && Number.isSafeInteger(p.pid) && p.pid > 0 &&
  text(p.pidStartTime) && text(p.bootId), "PID attribution needs machine, pid, pidStartTime, bootId");

export function lockPlan() {
  return {
    schema: "fabric-canary-lock-report/v1", verdict: "plan", dryRun: true,
    invariant: "T-LOCK: zero global mesh-root acquisitions from control delivery through broker ACK",
    classes: LOCK_CLASSES, workloads,
    completion: "E4 voids I-LOCK's completion/writeJsonAtomic control row: same-root native journal, no transport span.",
    collect: [
      "Inventory every participating owner PID, start time and boot id before the window; include replacements.",
      "Capture all-acquisition PID counters on the exact global mesh root, not only held/timeout events.",
      "Join delivery-to-broker-ACK spans to the same PID identities; distinguish owner-local control-seen locks.",
      "Record complete conformance, fault and canary coverage and zero append errors.",
    ],
    note: "Thresholded events.jsonl alone cannot prove zero acquisitions. No collector is executed here.",
  };
}

/** Pure normalized evidence evaluator. Counter collection and PID attribution belong to the caller. */
export function analyzeLockAttribution(evidence) {
  requireThat(evidence?.schema === "fabric-canary-lock/v1", "expected fabric-canary-lock/v1 evidence");
  const { window, expectedProcesses, records } = evidence;
  requireThat(window && text(window.meshRoot) && isAbsolute(window.meshRoot) && count(window.startAt) &&
    count(window.endAt) && window.endAt > window.startAt, "invalid attribution window or non-absolute global mesh root");
  requireThat(Array.isArray(expectedProcesses) && expectedProcesses.length > 0 && Array.isArray(records),
    "expectedProcesses must inventory the control owners; records must be an array");
  const expected = new Set();
  for (const p of expectedProcesses) {
    checkIdentity(p);
    requireThat(!expected.has(identity(p)), "duplicate expected PID identity");
    expected.add(identity(p));
  }
  const blockers = [], failures = [], seen = new Set(), coverage = new Set(), spans = new Set();
  if (evidence.ownerInventoryComplete !== true) blockers.push("owner PID inventory is not attested complete");
  const byClass = Object.fromEntries(LOCK_CLASSES.map((c) => [c, { spans: 0, acquisitions: 0 }]));
  const attribution = [];
  for (const record of records) {
    checkIdentity(record);
    const key = identity(record);
    requireThat(expected.has(key) && !seen.has(key), "unexpected or duplicate PID record");
    seen.add(key);
    requireThat(Array.isArray(record.spans) && count(record.globalAcquisitions) &&
      count(record.controlAcquisitions) && count(record.otherAcquisitions), "invalid PID acquisition counters");
    requireThat(record.globalAcquisitions === record.controlAcquisitions + record.otherAcquisitions,
      "every global acquisition must be attributed to control or explicitly outside control");
    requireThat(count(record.appendErrors), "appendErrors is required");
    if (record.meshRoot !== window.meshRoot || record.counterKind !== "all-acquisitions" ||
        record.complete !== true || record.startAt > window.startAt || record.endAt < window.endAt ||
        !Number.isSafeInteger(record.startAt) || !Number.isSafeInteger(record.endAt)) {
      blockers.push(`incomplete all-acquisition coverage for ${key}`);
    }
    if (record.appendErrors !== 0) blockers.push(`attribution lost events for ${key}`);
    let spanAcquisitions = 0;
    for (const span of record.spans) {
      requireThat(span && text(span.commandId) && LOCK_CLASSES.includes(span.targetClass) &&
        workloads.includes(span.workload) && count(span.globalAcquisitions) && count(span.startAt) && count(span.endAt) &&
        span.startAt >= window.startAt && span.endAt <= window.endAt && span.endAt >= span.startAt, "invalid control span or span outside attribution window; E4 completion is not a control class");
      // One delivery may be replayed: the capture must give every delivery attempt a distinct span id.
      requireThat(text(span.spanId) && !spans.has(`${key}:${span.spanId}`), "missing/duplicate spanId");
      spans.add(`${key}:${span.spanId}`);
      if (span.deliveryThroughBrokerAck !== true) blockers.push(`incomplete span ${span.spanId}`);
      coverage.add(`${span.workload}:${span.targetClass}`);
      byClass[span.targetClass].spans++;
      byClass[span.targetClass].acquisitions += span.globalAcquisitions;
      spanAcquisitions += span.globalAcquisitions;
    }
    requireThat(spanAcquisitions === record.controlAcquisitions, "control counters do not equal attributed span acquisitions");
    if (record.controlAcquisitions > 0) failures.push(`T-LOCK: ${record.controlAcquisitions} global control acquisitions for ${key}`);
    attribution.push({ machine: record.machine, pid: record.pid, pidStartTime: record.pidStartTime, bootId: record.bootId,
      globalAcquisitions: record.globalAcquisitions, controlAcquisitions: record.controlAcquisitions,
      otherAcquisitions: record.otherAcquisitions });
  }
  for (const key of expected) if (!seen.has(key)) blockers.push(`missing owner PID ${key}`);
  for (const workload of workloads) for (const target of LOCK_CLASSES) {
    if (!coverage.has(`${workload}:${target}`)) blockers.push(`missing ${workload}/${target} span coverage`);
  }
  return {
    schema: "fabric-canary-lock-report/v1", verdict: failures.length ? "fail" : blockers.length ? "blocked" : "pass",
    evidenceKind: "recorded-attribution", e2eProven: false, window, byClass, attribution, failures, blockers,
    note: "Other attributed PID activity is reported, not mistaken for handler acquisitions. Actor p95 is not claimed. E4 completion custody is unchanged and outside the control-span gate.",
  };
}

async function main(argv) {
  if (argv.includes("--help")) {
    console.log("Usage: node scripts/canary/lock-attribution.mjs [--dry-run | --input evidence.json]");
    return;
  }
  requireThat(argv.length === 0 || (argv.length === 1 && argv[0] === "--dry-run") ||
    (argv.length === 2 && argv[0] === "--input"), "use --dry-run or --input FILE");
  const report = argv[0] === "--input"
    ? analyzeLockAttribution(JSON.parse(await readFile(argv[1], "utf8"))) : lockPlan();
  console.log(JSON.stringify(report, null, 2));
  if (["fail", "blocked"].includes(report.verdict)) process.exitCode = 1;
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { await main(process.argv.slice(2)); }
  catch (error) { console.log(JSON.stringify({ verdict: "invalid", error: error.message })); process.exitCode = 2; }
}
