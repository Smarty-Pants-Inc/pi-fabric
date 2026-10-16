#!/usr/bin/env node
// T-P95: plan by default, or evaluate recorded sender-monotonic receipts. Never connects to NATS.
import { readFile } from "node:fs/promises";
import { isAbsolute, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { analyzeLockAttribution } from "./lock-attribution.mjs";

export const SUPPORTED_N = Object.freeze([1, 4, 16, 64]);
const targets = ["main", "task", "actor"];
const operations = Object.freeze(["followUp", "steer", "stop"]);
const requireThat = (test, message) => { if (!test) throw new Error(message); };
const text = (s) => typeof s === "string" && s.length > 0;
const count = (n) => Number.isSafeInteger(n) && n >= 0;
const processIdentity = (p) => p && Number.isSafeInteger(p.pid) && p.pid > 0 && text(p.pidStartTime) && text(p.bootId);
const ns = (s) => { requireThat(typeof s === "string" && /^(0|[1-9][0-9]*)$/.test(s), "monotonic ns must be decimal strings"); return BigInt(s); };
export const senderMonotonicNow = () => process.hrtime.bigint().toString();
export function elapsedMs(start, end) {
  const delta = ns(end) - ns(start);
  requireThat(delta >= 0n && delta <= BigInt(Number.MAX_SAFE_INTEGER), "invalid monotonic duration");
  return Number(delta) / 1e6;
}
/** Nearest-rank quantiles: no interpolation and no fabricated zero for an empty population. */
export function percentiles(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const q = (p) => sorted.length ? sorted[Math.ceil(p * sorted.length) - 1] : null;
  return { count: sorted.length, p50Ms: q(0.5), p95Ms: q(0.95), p99Ms: q(0.99) };
}
function selection(values) {
  requireThat(Array.isArray(values) && values.length > 0 && new Set(values).size === values.length &&
    values.every((n) => SUPPORTED_N.includes(n)), "N must be a unique subset of 1,4,16,64");
  return values;
}
export function p95Plan(n = SUPPORTED_N) {
  return {
    schema: "fabric-canary-p95-report/v1", verdict: "plan", dryRun: true, e2eProven: false, evidenceKind: "plan-only",
    server: { host: "intel1", version: "2.11.17", syncInterval: "always", replicas: 1, availability: "non-HA" },
    hosts: { WA: "ryzen2", WB: "ryzen4" },
    preconditions: ["P-HL: headless RPC Mains on both work hosts", "P-SRV: approved per-host credentials, private CA, dedicated messaging server",
      "P-UP: candidate artifact on both hosts, all runtimes NATS, no other fleet work", "P-ACL: work hosts can reach :4222"],
    taskPlacement: "Each host's headless Main spawns process tasks on its OWN host; cross-host flows are messaging only.",
    f3: { hosts: ["ryzen2", "ryzen4"], carriers: ["file", "nats"],
      requirement: "Parent offline at local child exit; native same-root journal, exact-parent delivered() receipt, unchanged in both modes." },
    steps: selection(n).map((senders) => ({ n: senders, durationMs: 600000, ratePerSenderHz: 1,
      realAgentsMax: 16, remainingSenders: "model-free Fabric control-plane processes", recipients: "real Main/process-task/actor",
      claimedOperations: [...operations], completionMeasurement: "report-only local settle -> delivered(); no transport hop" })),
    dispatcher: { inboxLanes: 8, maxInHandPerLane: 16, maxAckPending: 128, ackWaitMs: 30000,
      workingEveryMs: 10000, actorLane: "one serial lane", acksBypassLanes: true },
    measurement: "sender monotonic send -> authenticated owner ACK (B2), NOT command PubAck (B1)",
    reports: ["p50/p95/p99 per N and target class", "per-operation coverage", "KV writes/s", "PubAck p95 <= 250 ms",
      "B3 sender-observed receipt latency, separately and not claimed", "Completion settle -> delivered(), separately and not claimed; no transport hop"],
    gate: "healthy cross-host Main/process-task followUp, steer, stop p95 < 5000 ms (also <= 15000 ms); T-LOCK must pass",
    actor: "report only; never inherits the Main/task claim", bridge: "kept for non-control events",
    note: "No sends, processes, host changes or broker operations are executed by this CLI.",
  };
}

function checkContext(input) {
  requireThat(input?.schema === "fabric-canary-p95/v1", "expected fabric-canary-p95/v1 evidence");
  requireThat(Array.isArray(input.steps), "steps must be an array");
  const blockers = [];
  for (const key of ["P-HL", "P-SRV", "P-UP", "P-ACL"]) {
    const receipt = input.preconditions?.[key];
    if (receipt?.passed !== true || !text(receipt.receiptId)) blockers.push(`${key} needs a passing precondition receipt, not a boolean`);
  }
  if (input.mode !== "headless") blockers.push("transport-only run does not earn the headless claim");
  if (input.hosts?.WA !== "ryzen2" || input.hosts?.WB !== "ryzen4") blockers.push("work hosts do not match errata E3");
  if (input.server?.host !== "intel1" || input.server?.version !== "2.11.17" ||
      input.server?.syncInterval !== "always" || input.server?.replicas !== 1) blockers.push("server evidence does not match D6 and errata E2");
  return blockers;
}

/** Local completion timings are native-journal evidence, never command/ACK samples or a latency gate. */
function completionReport(samples = []) {
  requireThat(Array.isArray(samples), "completionSamples must be an array");
  const seen = new Set(), byHost = { ryzen2: [], ryzen4: [] }, latencies = [];
  for (const sample of samples) {
    requireThat(sample && text(sample.runId) && ["ryzen2", "ryzen4"].includes(sample.host) &&
      text(sample.meshRoot) && isAbsolute(sample.meshRoot) && text(sample.recipient?.rootId) && text(sample.recipient?.sessionId) &&
      processIdentity(sample.recorderProcess) && text(sample.clockId) && sample.settledClockId === sample.clockId &&
      sample.deliveredClockId === sample.clockId && text(sample.settlementReceiptId) && text(sample.deliveredReceiptId),
      "completion timing requires a local shared root, spawn-bound recipient, recorder clock and settlement/delivered() receipts");
    requireThat(!["operation", "commandId", "pubAckMonoNs", "pubAckSequence", "pubAckDurable", "ownerAckMonoNs",
      "ackAuthenticated", "ackCommandId", "ackSourceMachine", "ackOwnerHostId", "sourceMachine", "controlSeen", "carrier"]
      .some((key) => Object.hasOwn(sample, key)), "completion timing has no carrier, control-seen claim, command or transport ACK");
    const key = JSON.stringify([sample.host, sample.meshRoot, sample.runId]);
    requireThat(!seen.has(key), "duplicate native completion timing for a run"); seen.add(key);
    const latency = elapsedMs(sample.settledMonoNs, sample.deliveredMonoNs);
    latencies.push(latency); byHost[sample.host].push(latency);
  }
  return { ...percentiles(latencies), byHost: Object.fromEntries(Object.entries(byHost).map(([host, values]) => [host, percentiles(values)])),
    hostsMissing: Object.keys(byHost).filter((host) => byHost[host].length === 0), claimed: false, transportHop: false,
    meaning: "local settlement -> exact-parent inbox delivered(); unchanged same-root native journal, not owner ACK or B3" };
}

export function analyzeP95(input, requestedN = SUPPORTED_N) {
  selection(requestedN);
  const blockers = checkContext(input), failures = [], reports = [], seenN = new Set(), commandIds = new Set();
  let lockGate = { verdict: "blocked", blockers: ["missing T-LOCK evidence"] };
  if (input.lockEvidence !== undefined) lockGate = analyzeLockAttribution(input.lockEvidence);
  if (lockGate.verdict !== "pass") blockers.push(`T-LOCK ${lockGate.verdict}: the p95 claim is BLOCKED, never relaxed`);
  for (const step of input.steps) {
    requireThat(SUPPORTED_N.includes(step.n) && !seenN.has(step.n), "unsupported or duplicate N step");
    seenN.add(step.n);
    // Even non-selected evidence must have a supported, non-duplicated step number.
    if (!requestedN.includes(step.n)) continue;
    requireThat(count(step.durationMs) && step.durationMs > 0 && step.durationMs % 1000 === 0 &&
      Array.isArray(step.senders) && Array.isArray(step.recipients) && step.recipients.length > 0 &&
      Array.isArray(step.samples), "invalid step duration/senders/real-recipient inventory/samples");
    if (step.durationMs !== 600000 || step.ratePerSenderHz !== 1 || step.healthy !== true || step.complete !== true) {
      blockers.push(`N=${step.n}: needs a complete healthy 10-minute step at 1 command/s/sender`);
    }
    requireThat(step.senders.length === step.n, `N=${step.n}: sender inventory differs from N`);
    const senders = new Map(), slots = new Map(), senderProcesses = new Set(), senderClocks = new Set();
    for (const sender of step.senders) {
      requireThat(sender && text(sender.id) && text(sender.clockId) && !senders.has(sender.id) &&
        ["ryzen2", "ryzen4"].includes(sender.host) && ["real-agent", "model-free"].includes(sender.kind) &&
        processIdentity(sender.process) && text(sender.runtimeId) && text(sender.senderReceiptId), "invalid sender process/receipt inventory");
      const processKey = JSON.stringify([sender.host, sender.process.pid, sender.process.pidStartTime, sender.process.bootId]);
      requireThat(!senderProcesses.has(processKey) && !senderClocks.has(sender.clockId), "N counts distinct sender processes and clock domains, not renamed copies");
      senderProcesses.add(processKey); senderClocks.add(sender.clockId);
      if (sender.kind === "real-agent") requireThat(["main", "process-task"].includes(sender.agentKind),
        "real senders must be Fabric Mains or process tasks, not print roots or surrogate ids");
      requireThat(elapsedMs(sender.windowStartMonoNs, sender.windowEndMonoNs) === step.durationMs,
        "sender window must equal the recorded step duration in that sender's clock domain");
      senders.set(sender.id, sender);
      slots.set(sender.id, new Set());
    }
    const recipients = new Map();
    for (const recipient of step.recipients) {
      requireThat(recipient && text(recipient.id) && !recipients.has(recipient.id) && targets.includes(recipient.targetClass) &&
        ["ryzen2", "ryzen4"].includes(recipient.host) && text(recipient.runtimeId) && text(recipient.receiptId) &&
        processIdentity(recipient.process) && recipient.nativeKind === ({ main: "main", task: "process-task", actor: "actor" })[recipient.targetClass],
        "recipients must have real native process identities and readiness receipts");
      recipients.set(recipient.id, recipient);
    }
    const realAgents = step.senders.filter((s) => s.kind === "real-agent").length;
    if (realAgents < 1 || realAgents > 16) blockers.push(`N=${step.n}: expected 1..16 real agent senders`);
    requireThat(count(step.kvWrites) && Number.isFinite(step.kvWindowMs) && step.kvWindowMs > 0,
      "KV write count and observation window required");
    if (step.kvWindowMs !== step.durationMs || step.kvComplete !== true) blockers.push(`N=${step.n}: incomplete KV write-rate window`);
    const groups = Object.fromEntries(targets.map((target) => [target, { ack: [], pubAck: [], b3: [], total: 0,
      outcomes: { accepted: 0, rejected: 0, timeout: 0, uncertain: 0 }, operations: new Map() }]));
    const completion = completionReport(step.completionSamples);
    const pubAcks = [];
    for (const sample of step.samples) {
      const sender = senders.get(sample.senderId);
      requireThat(sender && sample.clockId === sender.clockId, "receipt timestamps must use the sending process's monotonic clock");
      requireThat(text(sample.commandId) && !commandIds.has(sample.commandId), "missing or duplicate commandId (retry attempts are not independent samples)");
      commandIds.add(sample.commandId);
      const recipient = recipients.get(sample.recipientId);
      requireThat(recipient && recipient.targetClass === sample.targetClass && recipient.host === sample.ownerHost &&
        targets.includes(sample.targetClass) && sample.ownerHost !== sender.host && sample.realRecipient === true,
        "samples require receipted real cross-host canary recipients");
      requireThat(sample.targetClass === "actor" ? ["tell", "ask", "stop"].includes(sample.operation) : operations.includes(sample.operation),
        "operation does not match target class; completion has no transport hop and belongs only in completionSamples");
      requireThat(["accepted", "rejected", "timeout", "uncertain"].includes(sample.outcome), "unknown sample outcome");
      const offset = elapsedMs(sender.windowStartMonoNs, sample.sendMonoNs);
      requireThat(offset < step.durationMs, "send lies outside sender window");
      const slot = Math.floor(offset / 1000), senderSlots = slots.get(sender.id);
      requireThat(!senderSlots.has(slot), "more than one command in a sender's one-second slot");
      senderSlots.add(slot);
      const group = groups[sample.targetClass];
      group.total++;
      group.outcomes[sample.outcome]++;
      const op = group.operations.get(sample.operation) ?? { total: 0, ack: [] };
      op.total++;
      group.operations.set(sample.operation, op);
      const hasAck = sample.ownerAckMonoNs !== undefined && sample.ownerAckMonoNs !== null;
      if (["accepted", "rejected"].includes(sample.outcome)) {
        requireThat(hasAck && sample.ackAuthenticated === true && sample.ackCommandId === sample.commandId &&
          sample.ackSourceMachine === recipient.host && sample.ackOwnerHostId === recipient.runtimeId,
          "an owner ACK must be authenticated, correlated to command and destination owner, and timestamped by its sender");
      } else requireThat(!hasAck, "timeout/uncertain sample cannot contain a settled owner ACK");
      if (hasAck) {
        const latency = elapsedMs(sample.sendMonoNs, sample.ownerAckMonoNs);
        group.ack.push(latency); op.ack.push(latency);
      }
      if (sample.pubAckMonoNs !== undefined && sample.pubAckMonoNs !== null) {
        requireThat(Number.isSafeInteger(sample.pubAckSequence) && sample.pubAckSequence > 0 && sample.pubAckDurable === true,
          "B1 timestamp requires a durable PubAck sequence, not an owner ACK");
        const latency = elapsedMs(sample.sendMonoNs, sample.pubAckMonoNs);
        group.pubAck.push(latency); pubAcks.push(latency);
      } else blockers.push(`N=${step.n}: missing command PubAck for ${sample.commandId}`);
      if (sample.b3MonoNs !== undefined && sample.b3MonoNs !== null) {
        requireThat(text(sample.b3ReceiptId), "B3 requires an existing native consumption receipt id");
        // This is when the sender observed that receipt, never a subtraction of remote clocks.
        group.b3.push(elapsedMs(sample.sendMonoNs, sample.b3MonoNs));
      }
      if (sample.outcome !== "accepted" && sample.targetClass !== "actor") failures.push(`N=${step.n}: ${sample.commandId} ${sample.outcome}`);
    }
    for (const [id, values] of slots) if (values.size !== step.durationMs / 1000) {
      blockers.push(`N=${step.n}: sender ${id} recorded ${values.size}/${step.durationMs / 1000} scheduled sends`);
    }
    const byTargetClass = {};
    for (const target of targets) {
      const group = groups[target], ack = percentiles(group.ack), byOperation = {};
      for (const [operation, values] of group.operations) {
        byOperation[operation] = { sent: values.total, ownerAck: percentiles(values.ack) };
        if (target !== "actor" && byOperation[operation].ownerAck.p95Ms !== null && byOperation[operation].ownerAck.p95Ms >= 5000) {
          failures.push(`N=${step.n}: ${target}/${operation} p95 is not < 5000 ms`);
        }
      }
      if (target !== "actor") {
        for (const op of operations) if (!byOperation[op]) blockers.push(`N=${step.n}: no ${target}/${op} samples`);
        if (ack.p95Ms !== null && ack.p95Ms >= 5000) failures.push(`N=${step.n}: ${target} p95 is not < 5000 ms`);
      }
      byTargetClass[target] = { sent: group.total, outcomes: group.outcomes, ownerAck: ack, byOperation,
        pubAck: percentiles(group.pubAck), b3: { ...percentiles(group.b3), missing: group.total - group.b3.length,
          claimed: false, meaning: "send -> sender observation of native B3 receipt; no remote wall-clock subtraction" },
        contractClaimedClass: target !== "actor", claimEligible: false, below5Seconds: ack.p95Ms === null ? null : ack.p95Ms < 5000,
        within15Seconds: ack.p95Ms === null ? null : ack.p95Ms <= 15000 };
    }
    const pubAck = percentiles(pubAcks);
    if (pubAck.p95Ms !== null && pubAck.p95Ms > 250) failures.push(`N=${step.n}: PubAck p95 > 250 ms; escalate, do not weaken sync_interval`);
    reports.push({ n: step.n, durationMs: step.durationMs, realAgents, modelFreeSenders: step.n - realAgents,
      byTargetClass, completion, kv: { writes: step.kvWrites, windowMs: step.kvWindowMs, writesPerSecond: step.kvWrites * 1000 / step.kvWindowMs }, pubAck });
  }
  for (const n of requestedN) if (!seenN.has(n)) blockers.push(`missing N=${n} step`);
  const metricVerdict = blockers.length ? "blocked" : failures.length ? "fail" : "pass";
  return { schema: "fabric-canary-p95-report/v1", verdict: metricVerdict === "pass" ? "probe-only" : metricVerdict,
    metricVerdict, evidenceKind: "recorded-probe-only", e2eProven: false, canaryLatencyClaimEligible: false,
    requestedN, fullRisingN: SUPPORTED_N.every((n) => requestedN.includes(n)), availability: "R1 non-HA", lockGate,
    steps: reports, failures, blockers, dispatcher: input.dispatcher ?? { status: "not-recorded" }, actorClaimed: false, b3Claimed: false, completionClaimed: false,
    proofLimit: "Recorded or injected receipts are evidence to review, not real E2E proof. Full headless and B1-B5 acceptance requires independently verified live receipts." };
}

/**
 * Optional trusted command adapter. CLI never loads or invokes one. The adapter owns the real
 * sender processes and records their monotonic timestamps; this process must not timestamp RPCs
 * on their behalf. A deadline ends observation, not broker custody; never retry with a new id.
 */
export async function collectP95(commandAdapter, context, n = SUPPORTED_N) {
  requireThat(typeof commandAdapter === "function", "inject a trusted command adapter");
  requireThat(checkContext({ ...context, steps: [] }).length === 0, "canary precondition receipts must pass before collection");
  requireThat(context.lockEvidence && analyzeLockAttribution(context.lockEvidence).verdict === "pass",
    "T-LOCK must pass before optional injected collection");
  const steps = [];
  for (const step of p95Plan(n).steps) {
    const controller = new AbortController();
    let timer;
    try {
      const result = await Promise.race([
        Promise.resolve().then(() => commandAdapter({ kind: "p95.collect-step", ...step,
          requireRealSenders: true, requireRealRecipients: true, retryWithNewId: false }, { signal: controller.signal })),
        new Promise((_, reject) => { timer = setTimeout(() => { controller.abort(); reject(new Error(`N=${step.n} observation deadline; outcome uncertain, retain command ids`)); }, step.durationMs + 120000); }),
      ]);
      requireThat(result?.n === step.n, "adapter returned the wrong N step");
      const checked = analyzeP95({ ...context, steps: [result] }, [step.n]);
      requireThat(checked.metricVerdict === "pass", `N=${step.n}: stop collection; ${JSON.stringify({ failures: checked.failures, blockers: checked.blockers })}`);
      steps.push(result);
    } finally { clearTimeout(timer); }
  }
  return analyzeP95({ ...context, steps }, n);
}

async function main(argv) {
  if (argv.includes("--help")) {
    console.log("Usage: node scripts/canary/p95.mjs [--dry-run] [--n 1,4,16,64] [--input receipts.json]"); return;
  }
  let inputFile, n = SUPPORTED_N, dryRun = false;
  const seen = new Set();
  for (let i = 0; i < argv.length; i++) {
    const flag = argv[i];
    requireThat(!seen.has(flag), `duplicate flag ${flag}`); seen.add(flag);
    if (flag === "--dry-run") dryRun = true;
    else if (flag === "--input" || flag === "--n") {
      const value = argv[++i]; requireThat(value && !value.startsWith("--"), `${flag} needs a value`);
      if (flag === "--input") inputFile = value; else n = selection(value.split(",").map(Number));
    } else throw new Error(`unknown flag ${flag}`);
  }
  requireThat(!(dryRun && inputFile), "--dry-run and --input are separate modes");
  const report = inputFile ? analyzeP95(JSON.parse(await readFile(inputFile, "utf8")), n) : p95Plan(n);
  console.log(JSON.stringify(report, null, 2));
  if (["fail", "blocked"].includes(report.verdict)) process.exitCode = 1;
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { await main(process.argv.slice(2)); }
  catch (error) { console.log(JSON.stringify({ verdict: "invalid", error: error.message })); process.exitCode = 2; }
}
