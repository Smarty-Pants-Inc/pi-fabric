// Recorded-call probes only: these check the evidence tools, not a real Fabric canary.
import assert from "node:assert/strict";
import { test } from "node:test";
import { analyzeP95, elapsedMs, p95Plan, percentiles } from "./p95.mjs";
import { analyzeLockAttribution } from "./lock-attribution.mjs";
import { recordedAdapter, rollbackPlan, runRollback } from "./rollback.mjs";

const lockEvidence = () => ({
  schema: "fabric-canary-lock/v1", ownerInventoryComplete: true,
  window: { meshRoot: "/recorded/mesh", startAt: 1000, endAt: 2000 },
  expectedProcesses: [{ machine: "ryzen2", pid: 1, pidStartTime: "1", bootId: "probe-boot" }],
  records: [{ machine: "ryzen2", pid: 1, pidStartTime: "1", bootId: "probe-boot", meshRoot: "/recorded/mesh",
    startAt: 1000, endAt: 2000, counterKind: "all-acquisitions", complete: true, appendErrors: 0,
    globalAcquisitions: 7, controlAcquisitions: 0, otherAcquisitions: 7,
    spans: ["conformance", "faults", "canary"].flatMap(workload => ["main", "task", "stop", "completion"].map(targetClass => ({
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
    const targetClass = index % 8 < 4 ? "main" : "task", sendMs = 1000 + index * 1000;
    const commandId = `recorded:${index}`;
    return { commandId, senderId: "a", clockId: "clock:a", recipientId: targetClass, ownerHost: "ryzen4", targetClass,
      operation: ["followUp", "steer", "stop", "completion"][index % 4], realRecipient: true,
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
});

test("T-P95 includes failed sends and blocks incomplete sampling instead of selecting survivors", () => {
  const data = p95Evidence();
  Object.assign(data.steps[0].samples[0], { outcome: "uncertain", ownerAckMonoNs: null });
  assert.equal(analyzeP95(data, [1]).metricVerdict, "fail");
  data.steps[0].samples.pop();
  assert.equal(analyzeP95(data, [1]).metricVerdict, "blocked");
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

test("T-ROLLBACK an uncertain pause stops before drain, flip or same-key retry", async () => {
  const actions = [];
  const result = await runRollback({ host: "ryzen4", runtimeIds: ["session:a"] }, async action => {
    actions.push(action.kind); throw new Error("injected uncertain pause");
  }, { dryRun: false, evidenceKind: "recorded-probe-only" });
  assert.deepEqual(actions, ["pause-admission"]);
  assert.equal(result.verdict, "blocked");
  assert.equal(result.phase, "pause");
  assert.equal(result.e2eProven, false);
  assert.equal(result.audit[0].status, "uncertain");
});

test("T-ROLLBACK recorded-call replay requires exact calls and refuses unused receipts", async () => {
  const adapter = recordedAdapter([{ action: { kind: "drain" }, result: { pending: 0 } }]);
  assert.throws(adapter.finish, /unused recorded calls/);
  await assert.rejects(adapter.execute({ kind: "flip-to-file" }), /does not match/);
});
