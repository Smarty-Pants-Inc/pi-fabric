// Recorded-call probes only: these check the evidence tools, not a real Fabric canary.
import assert from "node:assert/strict";
import { test } from "node:test";
import { createHash } from "node:crypto";
import { analyzeP95, collectP95, elapsedMs, p95Plan, percentiles } from "./p95.mjs";
import { analyzeLockAttribution, lockPlan, LOCK_CLASSES } from "./lock-attribution.mjs";
import { recordedAdapter, rollbackPlan, runRollback,
  ROLLBACK_DEADLINE_CAP_MS, ROLLBACK_RETRY_HORIZON_MS, ROLLBACK_HOLD_MS } from "./rollback.mjs";

const lockEvidence = () => ({
  schema: "fabric-canary-lock/v1", ownerInventoryComplete: true,
  window: { meshRoot: "/recorded/mesh", startAt: 1000, endAt: 2000 },
  expectedProcesses: [{ machine: "ryzen2", pid: 1, pidStartTime: "1", bootId: "probe-boot" }],
  records: [{ machine: "ryzen2", pid: 1, pidStartTime: "1", bootId: "probe-boot", meshRoot: "/recorded/mesh",
    startAt: 1000, endAt: 2000, counterKind: "all-acquisitions", complete: true, appendErrors: 0,
    globalAcquisitions: 7, controlAcquisitions: 0, otherAcquisitions: 7,
    spans: ["conformance", "faults", "canary"].flatMap(workload => ["main", "task", "stop"].map(targetClass => ({
      spanId: `${workload}:${targetClass}`, commandId: `${workload}:${targetClass}`, workload, targetClass,
      startAt: 1100, endAt: 1200, deliveryThroughBrokerAck: true, globalAcquisitions: 0,
    }))),
  }],
});
const ns = ms => String(BigInt(ms) * 1_000_000n);
const p95Evidence = () => {
  const senders = [{ id: "a", host: "ryzen2", kind: "real-agent", agentKind: "main", runtimeId: "session:a",
    senderReceiptId: "recorded:sender", process: { pid: 1, pidStartTime: "1", bootId: "probe-a" }, clockId: "clock:a",
    windowStartMonoNs: ns(1000), windowEndMonoNs: ns(601000) }];
  const recipients = ["main", "task"].map(targetClass => ({ id: targetClass, host: "ryzen4", targetClass,
    nativeKind: targetClass === "task" ? "process-task" : "main", runtimeId: "session:b", receiptId: `recorded:${targetClass}`,
    process: { pid: targetClass === "task" ? 3 : 2, pidStartTime: "1", bootId: "probe-b" } }));
  const samples = Array.from({ length: 600 }, (_, index) => {
    const targetClass = index % 6 < 3 ? "main" : "task", sendMs = 1000 + index * 1000;
    const commandId = `recorded:${index}`;
    return { commandId, senderId: "a", clockId: "clock:a", recipientId: targetClass, ownerHost: "ryzen4", targetClass,
      operation: ["followUp", "steer", "stop"][index % 3], realRecipient: true,
      sendMonoNs: ns(sendMs), pubAckMonoNs: ns(sendMs + 20), pubAckSequence: index + 1, pubAckDurable: true,
      ownerAckMonoNs: ns(sendMs + 100), outcome: "accepted", ackAuthenticated: true, ackCommandId: commandId,
      ackSourceMachine: "ryzen4", ackOwnerHostId: "session:b" };
  });
  return { schema: "fabric-canary-p95/v1", mode: "headless", hosts: { WA: "ryzen2", WB: "ryzen4" },
    server: { host: "intel1", version: "2.11.17", syncInterval: "always", replicas: 1 },
    preconditions: Object.fromEntries(["P-HL", "P-SRV", "P-UP", "P-ACL"].map(key => [key, { passed: true, receiptId: `recorded:${key}` }])),
    lockEvidence: lockEvidence(), steps: [{ n: 1, durationMs: 600000, ratePerSenderHz: 1, healthy: true, complete: true,
      kvWrites: 120, kvWindowMs: 600000, kvComplete: true, senders, recipients, samples }] };
};

// Keep reason: evidence trust boundary and rollback data safety. These fixtures are never E2E proof.
const completionSamples = () => ["ryzen2", "ryzen4"].map((host, index) => ({
  runId: `recorded:run:${host}`, host, meshRoot: `/recorded/${host}/mesh`,
  recipient: { rootId: `root:${host}`, sessionId: `parent:${host}` },
  recorderProcess: { pid: 10 + index, pidStartTime: "1", bootId: `recorded:${host}` },
  clockId: `completion-clock:${host}`, settledClockId: `completion-clock:${host}`, deliveredClockId: `completion-clock:${host}`,
  settledMonoNs: ns(1000), deliveredMonoNs: ns(index === 0 ? 26000 : 41000),
  settlementReceiptId: `recorded:settled:${host}`, deliveredReceiptId: `recorded:delivered:${host}`,
}));

const recordedStoreDir = (host, hostId) => `/recorded/${host}/control-seen/${createHash("sha256").update(hostId).digest("hex").slice(0, 32)}`;
// E20 authorizing probe fixture: INFO 0/0 only, no body inventory or relay transcript.
const rollbackTranscript = ({ host = "ryzen2", flippedAt = "2026-10-16T08:00:00.000Z" } = {}) => {
  const request = { host, runtimeIds: ["session:a", "runtime:b"],
    drainReceiptStores: ["session:a", "runtime:b"].map(hostId => ({ hostId, storeDir: recordedStoreDir(host, hostId) })) }, calls = [];
  const states = request.runtimeIds.map(runtimeId => ({ runtimeId, stream: `FABRIC_IN_${host}`, pending: 0, ackPending: 0 }));
  const add = (kind, payload, result) => calls.push({ action: { kind, host, runtimeIds: request.runtimeIds, ...payload }, result });
  add("pause-admission", {}, { paused: true, spawnsRefused: true, receiversDraining: true,
    code: "FABRIC_TRANSPORT_DRAINING", notRun: true, persisted: false, runtimeIds: request.runtimeIds, hostInventoryComplete: true });
  add("drain", { waitLimitMs: 120000 }, { elapsedMs: 120000, states });
  const writes = request.drainReceiptStores.map(({ hostId, storeDir }) => ({ storeDir, receipt: { version: 1, hostId,
    flippedAt: { from: "actual-flip-boundary", format: "epoch-ms" },
    completedAt: { from: "drain-completed-at-boundary", format: "epoch-ms" }, complete: true,
    consumer: { stream: `FABRIC_IN_${host}`, numPending: 0, numAckPending: 0 }, dispositions: [] } }));
  for (const write of writes) add("write-nats-drain-receipt", {
    module: "dist/topology/nats-drain-receipt.js", api: "writeNatsDrainReceipt", storeDir: write.storeDir,
    receipt: { ...write.receipt, flippedAt: Date.parse(flippedAt), completedAt: Date.parse(flippedAt) + 1 },
  }, { apiInvoked: true, persistence: "recorded-unverified" });
  add("flip-to-file", { transport: "file", reloadAll: true, retainBridge: true,
    hostConfigWrite: { atomic: true, set: { "mesh.transport": "file",
      "mesh.nats.rollbackHoldUntil": { from: "flippedAt", plusMs: 172800000, format: "utc-iso" } } },
    drainReceiptWrite: { module: "dist/topology/nats-drain-receipt.js", api: "writeNatsDrainReceipt",
      timing: "before-atomic-host-config-write", writes },
  }, {
    hostTransport: "file", fileRuntimeIds: request.runtimeIds, oldNatsWritersStopped: true, bridgeUp: true,
    filePresenceMirrored: true, eventTopicsUnaffected: true, fileAdmissionReady: true,
    hostConfigWrite: { atomic: true, persisted: true, flippedAt, set: { "mesh.transport": "file",
      "mesh.nats.rollbackHoldUntil": new Date(Date.parse(flippedAt) + 172800000).toISOString() } },
  });
  add("reconcile-command-ids", { commands: [], preserveControlSeen: true, maxSameKeyRetries: 0, retainCompletions: true }, {
    controlSeenStoreUnchanged: true, completionJournalRetained: true, completionJournalUnchanged: true, results: [],
  });
  add("retain-audit", {}, { streamsKept: true, consumersKept: true, fabricAdvKept: true,
    controlSeenKept: true, nativeCustodyKept: true, deletedOrPurged: false });
  return { schema: "fabric-canary-rollback-calls/v1", request, calls };
};
const replayRollback = async transcript => {
  const adapter = recordedAdapter(transcript.calls);
  const report = await runRollback(transcript.request, adapter.execute, { dryRun: false, evidenceKind: "recorded-probe-only" });
  if (report.verdict !== "blocked") adapter.finish();
  return report;
};

test("T-P95 nearest-rank statistics retain empty populations and sender-clock semantics", () => {
  assert.deepEqual(percentiles([]), { count: 0, p50Ms: null, p95Ms: null, p99Ms: null });
  assert.deepEqual(percentiles([1, 4, 2, 3]), { count: 4, p50Ms: 2, p95Ms: 4, p99Ms: 4 });
  assert.equal(elapsedMs("1000000000", "1001500000"), 1.5);
  assert.throws(() => elapsedMs("2", "1"), /invalid monotonic/);
  assert.deepEqual(p95Plan().steps.map(s => s.n), [1, 4, 16, 64]);
});

test("T-P95 a numerically passing recorded run is never live E2E proof", () => {
  const result = analyzeP95(p95Evidence(), [1]);
  assert.equal(result.metricVerdict, "pass");
  assert.equal(result.verdict, "probe-only");
  assert.equal(result.e2eProven, false);
  assert.equal(result.canaryLatencyClaimEligible, false);
  assert.equal(result.fullRisingN, false);
  assert.equal(result.steps[0].byTargetClass.main.ownerAck.p95Ms, 100);
  assert.equal(result.steps[0].byTargetClass.task.b3.missing, 300);
  assert.equal(result.steps[0].byTargetClass.actor.ownerAck.p95Ms, null);
  assert.equal(result.steps[0].completion.count, 0);
  assert.equal(result.steps[0].completion.p95Ms, null);
  assert.deepEqual(result.steps[0].completion.hostsMissing, ["ryzen2", "ryzen4"]);
  assert.equal(result.completionClaimed, false);
});

test("E4 plan scopes control p95 and local F3 to each host without remote spawn", () => {
  const plan = p95Plan([1]);
  assert.deepEqual(plan.steps[0].claimedOperations, ["followUp", "steer", "stop"]);
  assert.deepEqual(plan.f3.hosts, ["ryzen2", "ryzen4"]);
  assert.deepEqual(plan.f3.carriers, ["file", "nats"]);
  assert.match(plan.taskPlacement, /OWN host/);
  assert.equal(plan.e2eProven, false);
});

test("E4 completion latency is separate, report-only and never a PubAck/owner-ACK/B3 gate", () => {
  const data = p95Evidence(); data.steps[0].completionSamples = completionSamples();
  const report = analyzeP95(data, [1]), step = report.steps[0];
  assert.equal(report.metricVerdict, "pass");
  assert.equal(report.verdict, "probe-only");
  assert.equal(report.e2eProven, false);
  assert.equal(report.canaryLatencyClaimEligible, false);
  assert.equal(step.completion.p95Ms, 40000);
  assert.equal(step.completion.byHost.ryzen2.p95Ms, 25000);
  assert.equal(step.completion.byHost.ryzen4.p95Ms, 40000);
  assert.deepEqual(step.completion.hostsMissing, []);
  assert.equal(step.completion.claimed, false);
  assert.equal(step.completion.transportHop, false);
  assert.equal(step.pubAck.count, 600);
  for (const target of ["main", "task"]) {
    assert.equal(step.byTargetClass[target].ownerAck.count, 300);
    assert.deepEqual(Object.keys(step.byTargetClass[target].byOperation), ["followUp", "steer", "stop"]);
    assert.equal(step.byTargetClass[target].b3.count, 0);
  }
});

test("E4 rejects completion control samples and malformed native timing evidence", () => {
  for (const targetClass of ["main", "task", "actor"]) {
    const data = p95Evidence(), sample = data.steps[0].samples.find(s => s.targetClass === (targetClass === "actor" ? "main" : targetClass));
    if (targetClass === "actor") {
      Object.assign(data.steps[0].recipients[0], { targetClass: "actor", nativeKind: "actor" }); sample.targetClass = "actor";
    }
    sample.operation = "completion";
    assert.throws(() => analyzeP95(data, [1]), /completion has no transport hop/);
  }
  for (const mutation of [s => { s.deliveredClockId = "foreign-clock"; }, s => { s.meshRoot = "relative"; },
    s => { delete s.deliveredReceiptId; }, s => { s.deliveredMonoNs = "0"; }, s => { s.commandId = "completion:run"; },
    s => { s.carrier = "file"; }, s => { s.controlSeen = true; }, s => { s.ownerAckMonoNs = ns(2000); }]) {
    const data = p95Evidence(); data.steps[0].completionSamples = completionSamples(); mutation(data.steps[0].completionSamples[0]);
    assert.throws(() => analyzeP95(data, [1]), /completion timing|invalid monotonic/);
  }
  const data = p95Evidence(), sample = completionSamples()[0]; data.steps[0].completionSamples = [sample, structuredClone(sample)];
  assert.throws(() => analyzeP95(data, [1]), /duplicate native completion/);
});

test("E4 injected recorded collection uses amended control operations and remains probe-only", async () => {
  const data = p95Evidence(), { steps, ...context } = data;
  const report = await collectP95(async (action, { signal }) => {
    assert.equal(action.kind, "p95.collect-step");
    assert.deepEqual(action.claimedOperations, ["followUp", "steer", "stop"]);
    assert.equal(action.retryWithNewId, false);
    assert.equal(action.requireRealSenders, true);
    assert.equal(action.requireRealRecipients, true);
    assert.equal(signal.aborted, false);
    return { ...steps[0], completionSamples: completionSamples() };
  }, context, [1]);
  assert.equal(report.metricVerdict, "pass");
  assert.equal(report.e2eProven, false);
  assert.equal(report.canaryLatencyClaimEligible, false);
  assert.equal(report.steps[0].completion.claimed, false);
});

test("T-P95 includes failed sends and blocks incomplete sampling instead of selecting survivors", () => {
  const data = p95Evidence();
  Object.assign(data.steps[0].samples[0], { outcome: "uncertain", ownerAckMonoNs: null });
  assert.equal(analyzeP95(data, [1]).metricVerdict, "fail");
  data.steps[0].samples.pop();
  assert.equal(analyzeP95(data, [1]).metricVerdict, "blocked");
});

test("E4 control coverage, latency/PubAck thresholds and T-LOCK remain hard gates", () => {
  const missingOperation = p95Evidence();
  for (const sample of missingOperation.steps[0].samples) if (sample.targetClass === "task" && sample.operation === "steer") sample.operation = "followUp";
  const missingReport = analyzeP95(missingOperation, [1]);
  assert.equal(missingReport.metricVerdict, "blocked");
  assert.ok(missingReport.blockers.some(b => b.includes("no task/steer samples")));

  const slow = p95Evidence();
  for (const sample of slow.steps[0].samples) if (sample.targetClass === "task" && sample.operation === "stop") {
    sample.ownerAckMonoNs = String(BigInt(sample.sendMonoNs) + 5_000_000_000n);
  }
  const slowReport = analyzeP95(slow, [1]);
  assert.equal(slowReport.metricVerdict, "fail");
  assert.ok(slowReport.failures.some(f => f.includes("task/stop p95 is not < 5000 ms")));

  for (const [latencyMs, verdict] of [[250, "pass"], [251, "fail"]]) {
    const data = p95Evidence();
    for (const sample of data.steps[0].samples) sample.pubAckMonoNs = String(BigInt(sample.sendMonoNs) + BigInt(latencyMs) * 1_000_000n);
    assert.equal(analyzeP95(data, [1]).metricVerdict, verdict);
  }

  const locked = p95Evidence(), record = locked.lockEvidence.records[0];
  record.spans[0].globalAcquisitions = 1; record.controlAcquisitions = 1; record.globalAcquisitions = 8;
  const lockedReport = analyzeP95(locked, [1]);
  assert.equal(lockedReport.lockGate.verdict, "fail");
  assert.equal(lockedReport.metricVerdict, "blocked");
  assert.equal(lockedReport.e2eProven, false);
});

test("T-P95 rejects wrong ACK provenance and foreign clock IDs; T-LOCK cannot be a boolean", () => {
  for (const mutation of [s => { s.ackSourceMachine = "ryzen2"; }, s => { s.clockId = "clock:b"; }]) {
    const data = p95Evidence(); mutation(data.steps[0].samples[0]);
    assert.throws(() => analyzeP95(data, [1]), /ACK|clock/);
  }
  const data = p95Evidence(); delete data.lockEvidence;
  assert.equal(analyzeP95(data, [1]).metricVerdict, "blocked");
});

test("T-LOCK permits unrelated acquisitions but fails any delivery-through-ACK global acquisition", () => {
  const evidence = lockEvidence();
  assert.equal(analyzeLockAttribution(evidence).verdict, "pass");
  evidence.records[0].spans[0].globalAcquisitions = 1;
  evidence.records[0].controlAcquisitions = 1;
  evidence.records[0].globalAcquisitions = 8;
  assert.equal(analyzeLockAttribution(evidence).verdict, "fail");
});

test("E4 T-LOCK requires nine control combinations and rejects the void completion row", () => {
  assert.deepEqual(LOCK_CLASSES, ["main", "task", "stop"]);
  assert.deepEqual(lockPlan().classes, ["main", "task", "stop"]);
  const evidence = lockEvidence(), report = analyzeLockAttribution(evidence);
  assert.equal(evidence.records[0].spans.length, 9);
  assert.equal(report.verdict, "pass");
  assert.equal(Object.hasOwn(report.byClass, "completion"), false);
  evidence.records[0].spans[0].targetClass = "completion";
  assert.throws(() => analyzeLockAttribution(evidence), /E4 completion is not a control class/);
  const missing = lockEvidence(); missing.records[0].spans.pop();
  assert.equal(analyzeLockAttribution(missing).verdict, "blocked");
});

test("T-LOCK missing owners, lost events and incomplete spans block the zero-lock claim", () => {
  for (const mutation of [e => { e.records = []; }, e => { e.records[0].appendErrors = 1; },
    e => { e.records[0].spans[0].deliveryThroughBrokerAck = false; }]) {
    const evidence = lockEvidence(); mutation(evidence);
    assert.equal(analyzeLockAttribution(evidence).verdict, "blocked");
  }
});

test("T-ROLLBACK dry-run never invokes the injected command adapter and retains custody", async () => {
  let calls = 0;
  const request = { host: "ryzen2", runtimeIds: ["session:a"] };
  const result = await runRollback(request, () => { calls++; throw new Error("must not run"); });
  assert.equal(calls, 0);
  assert.deepEqual(result, rollbackPlan(request));
  assert.equal(result.steps.at(-1).action, "retain-audit");
  assert.ok(result.forbidden.includes("purge"));
});

// Keep reason: E14.3 evidence trust boundary/data safety; these are NOT live withdraw tests.
test("E14.3 T-ROLLBACK pins the normal 24h deadline cap plus H=24h to one 48h atomic flip", async () => {
  assert.equal(ROLLBACK_DEADLINE_CAP_MS, 86400000);
  assert.equal(ROLLBACK_RETRY_HORIZON_MS, 86400000);
  assert.equal(ROLLBACK_HOLD_MS, 172800000);
  assert.equal(ROLLBACK_HOLD_MS, ROLLBACK_DEADLINE_CAP_MS + ROLLBACK_RETRY_HORIZON_MS);
  const request = { host: "ryzen2", runtimeIds: ["session:a"] };
  const plan = await runRollback(request, () => { throw new Error("dry-run must not execute"); });
  assert.deepEqual(plan.withdrawHold, { deadlineCapMs: 86400000, retryHorizonMs: 86400000, durationMs: 172800000 });
  assert.deepEqual(plan.steps.find(s => s.phase === "flip").hostConfigWrite, { atomic: true, set: {
    "mesh.transport": "file", "mesh.nats.rollbackHoldUntil": { from: "flippedAt", plusMs: 172800000, format: "utc-iso" },
  } });
  assert.ok(plan.forbidden.includes("split transport/hold config writes"));
});

test("E14.3 T-ROLLBACK exact receipts anchor the hold to the actual flip, across UTC calendar boundaries", async () => {
  for (const [flippedAt, holdUntil] of [
    ["2026-10-16T08:00:00.000Z", "2026-10-18T08:00:00.000Z"],
    ["2026-12-31T23:59:59.999Z", "2027-01-02T23:59:59.999Z"],
    ["2028-02-28T23:59:59.123Z", "2028-03-01T23:59:59.123Z"],
  ]) {
    const report = await replayRollback(rollbackTranscript({ flippedAt }));
    assert.equal(report.verdict, "reconciled");
    assert.equal(report.evidenceKind, "recorded-probe-only");
    assert.equal(report.e2eProven, false);
    const flips = report.audit.filter(a => a.action.kind === "flip-to-file");
    assert.equal(flips.length, 1);
    assert.deepEqual(flips[0].result.hostConfigWrite, { atomic: true, persisted: true, flippedAt,
      set: { "mesh.transport": "file", "mesh.nats.rollbackHoldUntil": holdUntil } });
    assert.equal(Date.parse(holdUntil) - Date.parse(flippedAt), 172800000);
    assert.equal(report.reconciliation.completionJournalUnchanged, true);
  }
});

test("E14.3 T-ROLLBACK missing, malformed, stale or split config receipts fail typed/unknown before any retry", async () => {
  for (const mutation of [
    r => { delete r.hostConfigWrite; }, // Legacy FILE-only receipt is no longer sufficient.
    r => { delete r.hostConfigWrite.atomic; },
    r => { r.hostConfigWrite.atomic = false; },
    r => { delete r.hostConfigWrite.persisted; },
    r => { r.hostConfigWrite.persisted = false; },
    r => { delete r.hostConfigWrite.set["mesh.transport"]; },
    r => { r.hostConfigWrite.set["mesh.transport"] = "nats"; },
    r => { delete r.hostConfigWrite.set["mesh.nats.rollbackHoldUntil"]; },
    r => { r.hostConfigWrite.set["mesh.nats.rollbackHoldUntil"] = "garbage"; },
    r => { r.hostConfigWrite.set["mesh.nats.rollbackHoldUntil"] = 1792310400000; },
    r => { r.hostConfigWrite.set["mesh.nats.rollbackHoldUntil"] = "2026-10-18T08:00:00.000+00:00"; },
    r => { r.hostConfigWrite.set["mesh.nats.rollbackHoldUntil"] = "2026-10-18T08:00:00.000"; },
    r => { r.hostConfigWrite.set["mesh.nats.rollbackHoldUntil"] = "2026-02-30T08:00:00.000Z"; },
    r => { r.hostConfigWrite.set["mesh.nats.rollbackHoldUntil"] = "2026-10-15T08:00:00.000Z"; }, // Existing expired hold.
    r => { r.hostConfigWrite.set["mesh.nats.rollbackHoldUntil"] = "2026-10-17T08:00:00.000Z"; }, // Existing future hold, too short.
    r => { r.hostConfigWrite.set["mesh.nats.rollbackHoldUntil"] = "2026-10-18T07:59:59.999Z"; },
    r => { r.hostConfigWrite.set["mesh.nats.rollbackHoldUntil"] = "2026-10-19T08:00:00.000Z"; }, // Not exactly flip+48h.
    r => { delete r.hostConfigWrite.flippedAt; },
    r => { r.hostConfigWrite.flippedAt = "2026-02-30T08:00:00.000Z"; },
    r => { r.hostConfigWrite.flippedAt = "2026-10-16T08:00:00.000+01:00"; },
    r => { r.hostConfigWrite = { atomic: true, persisted: true, flippedAt: r.hostConfigWrite.flippedAt,
      transportWrite: { "mesh.transport": "file" },
      holdWrite: { "mesh.nats.rollbackHoldUntil": "2026-10-18T08:00:00.000Z" } }; },
    r => { r.code = "CONTROL_NOT_DELIVERED"; r.notDelivered = true; r.notRun = true; },
  ]) {
    const transcript = rollbackTranscript(); mutation(transcript.calls.find(c => c.action.kind === "flip-to-file").result);
    const report = await replayRollback(transcript);
    assert.equal(report.verdict, "blocked");
    assert.equal(report.phase, "flip");
    assert.equal(report.code, "FABRIC_CANARY_ROLLBACK_FLIP_UNVERIFIED");
    assert.equal(report.outcome, "outcome-unknown");
    assert.equal(Object.hasOwn(report, "notDelivered"), false);
    assert.equal(Object.hasOwn(report, "notRun"), false);
    assert.equal(Object.hasOwn(report, "reconciliation"), false);
    assert.equal(report.audit.at(-1).action.kind, "flip-to-file");
    assert.equal(report.e2eProven, false);
  }
});

test("E14.3 T-ROLLBACK exact replay rejects a legacy or partial flip action even with a valid hold receipt", async () => {
  for (const mutation of [
    a => { delete a.hostConfigWrite; },
    a => { delete a.hostConfigWrite.set["mesh.transport"]; },
    a => { delete a.hostConfigWrite.set["mesh.nats.rollbackHoldUntil"]; },
    a => { a.hostConfigWrite.atomic = false; },
    a => { a.hostConfigWrite.set["mesh.nats.rollbackHoldUntil"].plusMs = 86400000; },
  ]) {
    const transcript = rollbackTranscript(); mutation(transcript.calls.find(c => c.action.kind === "flip-to-file").action);
    const report = await replayRollback(transcript);
    assert.equal(report.verdict, "blocked");
    assert.equal(report.code, "FABRIC_CANARY_ROLLBACK_FLIP_UNVERIFIED");
    assert.equal(report.outcome, "outcome-unknown");
    assert.match(report.error, /does not match flip-to-file/);
    assert.equal(report.audit.at(-1).status, "uncertain");
    assert.equal(Object.hasOwn(report, "reconciliation"), false);
  }
});

test("E14.3 T-ROLLBACK an adapter failure cannot turn an uncertain flip into CONTROL_NOT_DELIVERED", async () => {
  const transcript = rollbackTranscript(), adapter = recordedAdapter(transcript.calls);
  const report = await runRollback(transcript.request, async (action, options) => {
    if (action.kind === "flip-to-file") throw Object.assign(new Error("config write status unknown"),
      { code: "CONTROL_NOT_DELIVERED", notDelivered: true, notRun: true });
    return adapter.execute(action, options);
  }, { dryRun: false, evidenceKind: "recorded-probe-only" });
  assert.equal(report.verdict, "blocked");
  assert.equal(report.code, "FABRIC_CANARY_ROLLBACK_FLIP_UNVERIFIED");
  assert.equal(report.outcome, "outcome-unknown");
  assert.equal(Object.hasOwn(report, "notDelivered"), false);
  assert.equal(Object.hasOwn(report, "notRun"), false);
  assert.equal(report.audit.at(-1).status, "uncertain");
  assert.equal(report.audit.at(-1).action.kind, "flip-to-file");
  assert.equal(report.e2eProven, false);
});

test("T-ROLLBACK an uncertain pause stops before drain, flip or same-key retry", async () => {
  const actions = [];
  const result = await runRollback({ host: "ryzen4", runtimeIds: ["session:a"],
    drainReceiptStores: [{ hostId: "session:a", storeDir: recordedStoreDir("ryzen4", "session:a") }] }, async action => {
    actions.push(action.kind); throw new Error("injected uncertain pause");
  }, { dryRun: false, evidenceKind: "recorded-probe-only" });
  assert.deepEqual(actions, ["pause-admission"]);
  assert.equal(result.verdict, "blocked");
  assert.equal(result.phase, "pause");
  assert.equal(result.e2eProven, false);
  assert.equal(result.audit[0].status, "uncertain");
});

// Keep reason: E19/E20 authority and data-safety boundary; probes are NOT live drain/release proof.
test("E20 plan omits classification/relay and limits reconciliation to empty inventory", () => {
  const plan = rollbackPlan({ host: "ryzen2", runtimeIds: ["session:a"] });
  assert.deepEqual(plan.steps.map(s => s.action), ["pause-admission", "drain", "write-nats-drain-receipt",
    "flip-to-file", "reconcile-command-ids", "retain-audit"]);
  assert.match(plan.steps.find(s => s.action === "drain").require, /INFO counters only/);
  assert.match(plan.steps.find(s => s.action === "reconcile-command-ids").require, /empty/);
  for (const action of ["inbox MSG.GET/DIRECT.GET", "classify-retained", "relay publish", "consumer create/delete", "automatic retry/unpause"]) {
    assert.ok(plan.forbidden.includes(action));
  }
});

test("E20 any nonzero counter on either runtime blocks before receipt/flip in probe AND live modes", async () => {
  for (const host of ["ryzen2", "ryzen4"]) for (const index of [0, 1]) for (const field of ["pending", "ackPending"]) {
    for (const drainReceiptMode of ["probe", "live"]) for (const elapsed of [0, 120000]) {
      const transcript = rollbackTranscript({ host }), seen = [];
      const drain = transcript.calls.find(c => c.action.kind === "drain").result;
      drain.states[index][field] = 1; drain.elapsedMs = elapsed;
      const adapter = recordedAdapter(transcript.calls);
      const report = await runRollback(transcript.request, async (action, options) => {
        seen.push(action.kind);
        return adapter.execute(action, options);
      }, { dryRun: false, evidenceKind: "injected-unverified", drainReceiptMode });
      assert.equal(report.verdict, "blocked");
      assert.equal(report.phase, "drain");
      assert.equal(report.code, "FABRIC_CANARY_ROLLBACK_DRAIN_NOT_EMPTY");
      assert.equal(report.outcome, "outcome-unknown");
      assert.deepEqual(seen, ["pause-admission", "drain"]);
      assert.deepEqual(report.audit.map(a => a.action.kind), seen);
      assert.deepEqual(report.drainStates, drain.states);
      assert.deepEqual(report.drainReceiptWrites, []);
      assert.deepEqual(report.dispositions, []);
      assert.equal(Object.hasOwn(report, "reconciliation"), false);
      assert.equal(Object.hasOwn(report, "notRun"), false);
      assert.equal(Object.hasOwn(report, "notDelivered"), false);
      assert.equal(report.e2eProven, false); assert.equal(report.releaseProven, false);
      assert.match(report.error, /owner to drain \(or restart it\)/);
      assert.match(report.error, /new explicit operator action/);
      assert.match(report.error, /owner cannot run, a person decides/);
      assert.match(report.recovery, /No auto-resume, retry, unpause or cleanup/);
      assert.throws(adapter.finish, /unused recorded calls/);
    }
  }
});

test("E20 historical classification/relay calls are non-authorizing and stay unconsumed", async () => {
  for (const nonzero of [false, true]) {
    const transcript = rollbackTranscript();
    if (nonzero) transcript.calls[1].result.states[0].ackPending = 1;
    // Negative-only legacy sentinels, not an executable historical relay fixture.
    transcript.calls.splice(2, 0,
      { action: { kind: "classify-retained", host: "ryzen2", runtimeIds: transcript.request.runtimeIds }, result: { complete: true } },
      { action: { kind: "relay-outcome-unknown", host: "ryzen2", runtimeIds: transcript.request.runtimeIds }, result: { persisted: true } });
    const adapter = recordedAdapter(transcript.calls), seen = [];
    const report = await runRollback(transcript.request, async (action, options) => {
      seen.push(action.kind); return adapter.execute(action, options);
    }, { dryRun: false, evidenceKind: "recorded-probe-only" });
    assert.equal(report.verdict, "blocked");
    assert.equal(seen.includes("classify-retained"), false);
    assert.equal(seen.includes("relay-outcome-unknown"), false);
    assert.deepEqual(report.drainReceiptWrites, []);
    assert.equal(Object.hasOwn(report, "reconciliation"), false);
    assert.throws(adapter.finish, /unused recorded calls/);
    if (nonzero) assert.deepEqual(seen, ["pause-admission", "drain"]);
  }
});

test("E4 T-ROLLBACK refuses completion resends or changed/unretained journals", async () => {
  for (const mutation of [r => { r.completionResendCarrier = "file"; }, r => { r.completionJournalRetained = false; },
    r => { r.completionJournalUnchanged = false; }, r => { delete r.completionJournalUnchanged; }]) {
    const transcript = rollbackTranscript(); mutation(transcript.calls.find(c => c.action.kind === "reconcile-command-ids").result);
    const report = await replayRollback(transcript);
    assert.equal(report.verdict, "blocked");
    assert.equal(report.phase, "reconcile");
    assert.match(report.error, /native completion journal stays unchanged/);
    assert.equal(report.audit.at(-1).action.kind, "reconcile-command-ids");
  }
});

test("E20 preserves old-writer, empty-reconciliation and audit safety gates", async () => {
  for (const [kind, mutation, phase] of [
    ["flip-to-file", r => { r.oldNatsWritersStopped = false; }, "flip"],
    ["flip-to-file", r => { r.fileAdmissionReady = false; }, "flip"],
    ["flip-to-file", r => { r.fileRuntimeIds = r.fileRuntimeIds.slice(0, 1); }, "flip"],
    ["reconcile-command-ids", r => { r.results.push({ commandId: "unexpected", sameKeyRetries: 1 }); }, "reconcile"],
    ["reconcile-command-ids", r => { r.controlSeenStoreUnchanged = false; }, "reconcile"],
    ["retain-audit", r => { r.deletedOrPurged = true; }, "retain"],
  ]) {
    const transcript = rollbackTranscript(); mutation(transcript.calls.find(c => c.action.kind === kind).result);
    const report = await replayRollback(transcript);
    assert.equal(report.verdict, "blocked");
    assert.equal(report.phase, phase);
    assert.equal(report.audit.at(-1).action.kind, kind);
    assert.equal(report.e2eProven, false); assert.equal(report.releaseProven, false);
  }
});

// Keep reason: E17 writer trust boundary/local custody safety, NOT fsync or release-predicate proof.
test("E20 0/0 exact replay writes empty complete E17 receipts before atomic flip, then reconciles nothing", async () => {
  for (const host of ["ryzen2", "ryzen4"]) for (const elapsed of [0, 120000]) {
    const transcript = rollbackTranscript({ host }); transcript.calls[1].result.elapsedMs = elapsed;
    const report = await replayRollback(transcript);
    assert.equal(report.verdict, "reconciled");
    assert.equal(report.evidenceKind, "recorded-probe-only");
    assert.equal(report.e2eProven, false); assert.equal(report.releaseProven, false);
    assert.equal(report.fullyDrained, true); assert.equal(report.drainReceiptMode, "probe");
    assert.deepEqual(report.dispositions, []);
    assert.deepEqual(report.audit.map(a => a.action.kind), ["pause-admission", "drain",
      "write-nats-drain-receipt", "write-nats-drain-receipt", "flip-to-file", "reconcile-command-ids", "retain-audit"]);
    assert.equal(report.audit.length, transcript.calls.length);
    assert.equal(report.drainReceiptWrites.length, transcript.request.runtimeIds.length);
    for (const write of report.drainReceiptWrites) {
      const configured = transcript.request.drainReceiptStores.find(s => s.hostId === write.receipt.hostId);
      assert.equal(write.storeDir, configured.storeDir);
      // Independent expected numeric (storeDir, NatsDrainReceipt) => void API argument.
      assert.deepEqual(write.receipt, {
        version: 1, hostId: configured.hostId,
        flippedAt: Date.parse("2026-10-16T08:00:00.000Z"),
        completedAt: Date.parse("2026-10-16T08:00:00.000Z") + 1, complete: true,
        consumer: { stream: `FABRIC_IN_${host}`, numPending: 0, numAckPending: 0 }, dispositions: [],
      });
      assert.equal(write.persistence, "recorded-unverified");
      const call = report.audit.find(a => a.action.kind === "write-nats-drain-receipt" && a.action.storeDir === write.storeDir);
      assert.deepEqual(call.action, { kind: "write-nats-drain-receipt", host, runtimeIds: transcript.request.runtimeIds,
        module: "dist/topology/nats-drain-receipt.js", api: "writeNatsDrainReceipt", storeDir: write.storeDir, receipt: write.receipt });
      assert.deepEqual(call.result, { apiInvoked: true, persistence: "recorded-unverified" });
    }
    const reconciliation = report.audit.find(a => a.action.kind === "reconcile-command-ids");
    assert.deepEqual(reconciliation.action, { kind: "reconcile-command-ids", host, runtimeIds: transcript.request.runtimeIds,
      commands: [], preserveControlSeen: true, maxSameKeyRetries: 0, retainCompletions: true });
    assert.deepEqual(report.reconciliation.results, []);
    assert.equal(report.reconciliation.completionJournalRetained, true);
    assert.equal(report.reconciliation.completionJournalUnchanged, true);
    assert.equal(Object.hasOwn(report.reconciliation, "completionResendCarrier"), false);
  }
});

test("E20 malformed or incomplete INFO drain receipts never reach the writer or flip", async () => {
  for (const mutation of [
    r => { r.states[0].pending = -1; }, r => { r.states[1].ackPending = 0.5; },
    r => { r.states[0].pending = Number.MAX_SAFE_INTEGER + 1; }, r => { delete r.states[1].ackPending; },
    r => { delete r.states[0].stream; }, r => { r.states[0].stream = "FABRIC_IN_ryzen4"; },
    r => { r.states.pop(); }, r => { r.states[1] = r.states[0]; },
    r => { r.states[1].runtimeId = "foreign:runtime"; },
    r => { r.elapsedMs = 120001; }, r => { r.elapsedMs = -1; },
  ]) {
    const transcript = rollbackTranscript(); mutation(transcript.calls[1].result);
    const report = await replayRollback(transcript);
    assert.equal(report.verdict, "blocked"); assert.equal(report.phase, "drain");
    assert.equal(report.releaseProven, false);
    assert.deepEqual(report.drainReceiptWrites, []);
    assert.deepEqual(report.audit.map(a => a.action.kind), ["pause-admission", "drain"]);
  }
});

test("E17 uncertain second writer keeps the first receipt audit but never consumes a config flip or retries", async () => {
  const transcript = rollbackTranscript(), adapter = recordedAdapter(transcript.calls);
  const report = await runRollback(transcript.request, async (action, options) => {
    if (action.kind === "write-nats-drain-receipt" && action.receipt.hostId === "runtime:b") {
      throw new Error("recorded writer persistence uncertain");
    }
    return adapter.execute(action, options);
  }, { dryRun: false, evidenceKind: "recorded-probe-only" });
  assert.equal(report.verdict, "blocked");
  assert.equal(report.phase, "drain-receipt");
  assert.equal(report.code, "FABRIC_CANARY_ROLLBACK_DRAIN_RECEIPT_UNVERIFIED");
  assert.equal(report.drainReceiptWrites.length, 1);
  assert.equal(report.drainReceiptWrites[0].receipt.hostId, "session:a");
  assert.equal(report.drainReceiptWrites[0].persistence, "recorded-unverified");
  assert.equal(report.audit.filter(a => a.action.kind === "write-nats-drain-receipt").length, 2);
  assert.equal(report.audit.at(-1).status, "uncertain");
  assert.equal(Object.hasOwn(report.audit.at(-1), "result"), false);
  assert.equal(Object.hasOwn(report, "reconciliation"), false);
  assert.equal(report.releaseProven, false);
});

test("E17 missing, mesh-root, relative, duplicate and foreign-host store bindings fail before pause", async () => {
  for (const mutation of [
    r => { delete r.drainReceiptStores; },
    r => { r.drainReceiptStores.pop(); },
    r => { r.drainReceiptStores[0].storeDir = "/recorded/ryzen2/mesh"; },
    r => { r.drainReceiptStores[0].storeDir = "control-seen/relative"; },
    r => { r.drainReceiptStores[0].storeDir = recordedStoreDir("ryzen2", "foreign:host"); },
    r => { r.drainReceiptStores[0].hostId = "foreign:host"; },
    r => { r.drainReceiptStores[1] = r.drainReceiptStores[0]; },
  ]) {
    const transcript = rollbackTranscript(); mutation(transcript.request);
    const report = await replayRollback(transcript);
    assert.equal(report.verdict, "blocked");
    assert.equal(report.code, "FABRIC_CANARY_ROLLBACK_DRAIN_RECEIPT_UNVERIFIED");
    assert.deepEqual(report.audit, []);
    assert.deepEqual(report.drainReceiptWrites, []);
  }
});

test("E17 exact replay rejects foreign, stale-time, incomplete and falsely persisted writer receipts", async () => {
  for (const mutation of [
    c => { c.action.storeDir = recordedStoreDir("ryzen2", "foreign:host"); },
    c => { c.action.receipt.hostId = "foreign:host"; },
    c => { c.action.receipt.complete = false; },
    c => { c.action.receipt.consumer.numPending = 1; },
    c => { c.action.receipt.dispositions.push({ commandEventId: "legacy:event", disposition: "relay" }); },
    c => { c.action.receipt.completedAt = c.action.receipt.flippedAt - 1; },
    c => { c.action.receipt.flippedAt = -1; },
    c => { c.result = { apiInvoked: true, persistence: "built-writer-returned" }; },
    c => { c.result.persisted = true; },
    c => { c.result.apiInvoked = false; },
  ]) {
    const transcript = rollbackTranscript(); mutation(transcript.calls.find(c => c.action.kind === "write-nats-drain-receipt"));
    const report = await replayRollback(transcript);
    assert.equal(report.verdict, "blocked");
    assert.equal(report.releaseProven, false);
    assert.equal(Object.hasOwn(report, "reconciliation"), false);
    assert.equal(report.audit.at(-1).status, "uncertain");
  }
});

test("E17 receipt timestamp is bound to actual E14.3 config flip, not a planned earlier flip", async () => {
  const transcript = rollbackTranscript();
  const config = transcript.calls.find(c => c.action.kind === "flip-to-file").result.hostConfigWrite;
  config.flippedAt = "2026-10-16T08:00:01.000Z";
  config.set["mesh.nats.rollbackHoldUntil"] = "2026-10-18T08:00:01.000Z";
  const report = await replayRollback(transcript);
  assert.equal(report.verdict, "blocked");
  assert.equal(report.code, "FABRIC_CANARY_ROLLBACK_FLIP_UNVERIFIED");
  assert.match(report.error, /drain receipt flippedAt must equal/);
  assert.equal(report.releaseProven, false);
});

test("E17 driver cannot skip or duplicate the mandatory beforeFlip hook", async () => {
  for (const duplicate of [false, true]) {
    const transcript = rollbackTranscript(), adapter = recordedAdapter(transcript.calls);
    const flipResult = transcript.calls.find(c => c.action.kind === "flip-to-file").result;
    const report = await runRollback(transcript.request, async (action, options) => {
      if (action.kind !== "flip-to-file") return adapter.execute(action, options);
      if (duplicate) {
        const stamp = { flippedAt: flipResult.hostConfigWrite.flippedAt, completedAt: Date.parse(flipResult.hostConfigWrite.flippedAt) + 1 };
        await options.beforeFlip(stamp);
        await assert.rejects(options.beforeFlip(stamp), /exactly once/);
      }
      return structuredClone(flipResult);
    }, { dryRun: false, evidenceKind: "recorded-probe-only" });
    assert.equal(report.verdict, "blocked");
    assert.equal(report.releaseProven, false);
    assert.equal(Object.hasOwn(report, "reconciliation"), false);
  }
});

test("E17 replay cannot opt into the live built writer, even with a misleading evidenceKind", async () => {
  const transcript = rollbackTranscript(), adapter = recordedAdapter(transcript.calls);
  const report = await runRollback(transcript.request, adapter.execute,
    { dryRun: false, evidenceKind: "injected-unverified", drainReceiptMode: "live" });
  assert.equal(report.verdict, "blocked");
  assert.match(report.error, /recorded replay cannot opt into live/);
  assert.equal(report.releaseProven, false);
  assert.deepEqual(report.audit, []);
  assert.deepEqual(report.drainReceiptWrites, []);
});

test("T-ROLLBACK recorded-call replay requires exact calls and refuses unused receipts", async () => {
  const adapter = recordedAdapter([{ action: { kind: "drain" }, result: { pending: 0 } }]);
  assert.throws(adapter.finish, /unused recorded calls/);
  await assert.rejects(adapter.execute({ kind: "flip-to-file" }), /does not match/);
  const transcript = rollbackTranscript();
  // An appended historical relay is not silently accepted after a valid 0/0 replay either.
  transcript.calls.push({ action: { kind: "relay-outcome-unknown" }, result: { persisted: true } });
  await assert.rejects(replayRollback(transcript), /unused recorded calls/);
});
