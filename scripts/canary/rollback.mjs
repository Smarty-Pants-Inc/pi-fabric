#!/usr/bin/env node
// Frozen contract §8.3. The CLI only prints plans or replays JSON receipts.
import { readFile } from "node:fs/promises";
import { basename, dirname, isAbsolute, resolve } from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";

const requireThat = (test, message) => { if (!test) throw new Error(message); };
const text = (s) => typeof s === "string" && s.length > 0;
const count = (n) => Number.isSafeInteger(n) && n >= 0;
const sameSet = (a, b) => Array.isArray(a) && a.length === b.length && new Set(a).size === a.length && b.every((v) => a.includes(v));
const recordedExecutors = new WeakSet();
const DRAIN_WRITER_MODULE = "dist/topology/nats-drain-receipt.js";
const DRAIN_WRITER_API = "writeNatsDrainReceipt";
const blocked = (code, message) => Object.assign(new Error(message), { rollbackCode: code });
const drainBlocked = (message) => blocked("FABRIC_CANARY_ROLLBACK_DRAIN_RECEIPT_UNVERIFIED", message);
const epoch = (value) => count(value) && Number.isFinite(new Date(value).getTime());

function validateDrainStores(request) {
  const stores = request.drainReceiptStores;
  requireThat(Array.isArray(stores) && stores.every((s) => s && text(s.hostId) && text(s.storeDir)) &&
    sameSet(stores.map((s) => s.hostId), request.runtimeIds),
    "drainReceiptStores must explicitly map EVERY runtime hostId to its same-host control-seen storeDir");
  for (const { hostId, storeDir } of stores) {
    const hash = createHash("sha256").update(hostId).digest("hex").slice(0, 32);
    requireThat(isAbsolute(storeDir) && resolve(storeDir) === storeDir && basename(storeDir) === hash &&
      basename(dirname(storeDir)) === "control-seen",
      `storeDir for ${hostId} must be the exact absolute control-seen/<sha256(hostId)[0:32]> directory, not a mesh root or another host's store`);
  }
  return stores.map(({ hostId, storeDir }) => ({ hostId, storeDir }));
}

function receiptWritePlan(stores, states) {
  return stores.map(({ hostId, storeDir }) => {
    const state = states.find((s) => s.runtimeId === hostId);
    requireThat(state.pending === 0 && state.ackPending === 0, "E20 complete receipt requires actual 0/0 counters");
    return { storeDir, receipt: { version: 1, hostId,
      flippedAt: { from: "actual-flip-boundary", format: "epoch-ms" },
      completedAt: { from: "drain-completed-at-boundary", format: "epoch-ms" }, complete: true,
      consumer: { stream: state.stream, numPending: 0, numAckPending: 0 }, dispositions: [] } };
  });
}

// Frozen §3.3 + E14.3: latest NATS expiresAt = flip + deadline cap + H.
export const ROLLBACK_DEADLINE_CAP_MS = 24 * 60 * 60 * 1000;
export const ROLLBACK_RETRY_HORIZON_MS = 24 * 60 * 60 * 1000;
export const ROLLBACK_HOLD_MS = ROLLBACK_DEADLINE_CAP_MS + ROLLBACK_RETRY_HORIZON_MS;
const fileConfigWrite = () => ({ atomic: true, set: {
  "mesh.transport": "file",
  "mesh.nats.rollbackHoldUntil": { from: "flippedAt", plusMs: ROLLBACK_HOLD_MS, format: "utc-iso" },
} });
const utcMillis = (value) => {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value)) return NaN;
  const ms = Date.parse(value);
  return Number.isFinite(ms) && new Date(ms).toISOString() === value ? ms : NaN;
};
function validateFileConfigWrite(write) {
  requireThat(write?.atomic === true && write.persisted === true,
    "E14.3 flip requires one atomic persisted host-config write containing BOTH FILE and rollbackHoldUntil");
  const flippedAt = utcMillis(write.flippedAt), hold = utcMillis(write.set?.["mesh.nats.rollbackHoldUntil"]);
  requireThat(Number.isFinite(flippedAt) && Number.isFinite(hold) && hold - flippedAt === ROLLBACK_HOLD_MS &&
    isDeepStrictEqual(write.set, { "mesh.transport": "file", "mesh.nats.rollbackHoldUntil": new Date(hold).toISOString() }),
    "E14.3 host-config receipt must set BOTH mesh.transport=file and mesh.nats.rollbackHoldUntil=flippedAt+48h (canonical UTC ISO); missing, malformed or stale holds are not proof");
}

function validateRequest(request) {
  requireThat(request && ["ryzen2", "ryzen4"].includes(request.host), "rollback host must be ryzen2 or ryzen4 (errata E3)");
  requireThat(Array.isArray(request.runtimeIds) && request.runtimeIds.length > 0 &&
    request.runtimeIds.every(text) && new Set(request.runtimeIds).size === request.runtimeIds.length,
    "runtimeIds must inventory every runtime on the host");
}
export function rollbackPlan(request) {
  validateRequest(request);
  return {
    schema: "fabric-canary-rollback-plan/v1", verdict: "plan", dryRun: true, request,
    steps: [
      { phase: "pause", action: "pause-admission", host: request.host, runtimeIds: request.runtimeIds,
        require: "new spawns refused; new sends FABRIC_TRANSPORT_DRAINING/notRun before persistence; receivers continue draining" },
      { phase: "drain", action: "drain", waitLimitMs: 120000,
        require: "INFO counters only: pending=0, ackPending=0 on EVERY runtime; any nonzero result is BLOCKED/outcome-unknown, preserving pause and audit for a new explicit operator retry after the owner drains or restarts; if the owner cannot run, a person decides" },
      { phase: "drain-receipt", action: "write-nats-drain-receipt", module: DRAIN_WRITER_MODULE, api: DRAIN_WRITER_API,
        require: "mandatory beforeFlip hook inside flip-to-file, AFTER actual 0/0 drain and BEFORE its atomic config write; complete:true, dispositions:[]; explicit drainReceiptStores per runtime hostId; built writer only in live mode, exact API-call replay with NO writes in probe mode" },
      { phase: "flip", action: "flip-to-file", hostConfigWrite: fileConfigWrite(),
        require: "ONE atomic host-config write sets mesh.transport=file AND mesh.nats.rollbackHoldUntil=actual flip+48h UTC ISO (24h deadline cap + H=24h); reload ALL runtimes with that hold, confirm old NATS writers stopped before file admission is ready; bridge stays up and resumes file presence mirrors" },
      { phase: "reconcile", action: "reconcile-command-ids", require: "empty command inventory and empty results only; no retry; same control-seen store and native same-root completion journal retained unchanged; no completion carrier replay" },
      { phase: "retain", action: "retain-audit", require: "keep streams, consumers, FABRIC_ADV, control-seen and ALL native custody/consumption journals" },
    ],
    withdrawHold: { deadlineCapMs: ROLLBACK_DEADLINE_CAP_MS, retryHorizonMs: ROLLBACK_RETRY_HORIZON_MS, durationMs: ROLLBACK_HOLD_MS },
    forbidden: ["delete", "purge", "ack remaining messages as cleanup", "new-id retry", "automatic TTL takeover", "stop event bridge",
      "split transport/hold config writes", "reuse an existing rollback hold without refreshing to flip+48h",
      "completion carrier replay", "completion control-seen claim", "inbox MSG.GET/DIRECT.GET", "classify-retained",
      "relay publish", "consumer create/delete", "automatic retry/unpause"],
    note: "This is a per-host control/presence rollback, not fleet cutover or file-surface retirement. E4 completions have no transport rollback; native envelopes, attempts, receipts, archives and claims stay unchanged.",
  };
}

/** Exact recorded-call adapter: no dynamic code, shell, broker access, or writes. */
export function recordedAdapter(calls) {
  requireThat(Array.isArray(calls), "calls must be an array");
  let index = 0;
  const execute = async (action, { beforeFlip } = {}) => {
    if (action.kind === "flip-to-file" && beforeFlip) {
      const receipt = calls[index]?.action?.receipt;
      requireThat(receipt && epoch(receipt.flippedAt), "recorded flip is missing its pre-flip writer API call");
      await beforeFlip({ flippedAt: new Date(receipt.flippedAt).toISOString(), completedAt: receipt.completedAt });
    }
    const call = calls[index++];
    requireThat(call && isDeepStrictEqual(call.action, action), `recorded call ${index} does not match ${action.kind}`);
    requireThat(Object.hasOwn(call, "result"), `recorded call ${index} has no result`);
    return structuredClone(call.result);
  };
  recordedExecutors.add(execute);
  return { execute, finish: () => requireThat(index === calls.length, "unused recorded calls; transcript is not an exact replay") };
}

/**
 * Side-effect boundary: a trusted, explicitly injected adapter receives structured actions.
 * Only its mandatory beforeFlip hook may invoke A1's built receipt writer in explicit live mode.
 * It must respect AbortSignal and own finite host/CLI deadlines. Aborting observation cannot undo
 * a persisted action; on error this function stops and returns its partial audit, never auto-retries.
 * flip-to-file must resolve the hold expression at the ACTUAL config flip, not plan/start time,
 * atomically persist both set fields in ONE host-config write, then reload every runtime with it.
 * Its hostConfigWrite receipt contains atomic/persisted=true, flippedAt and the concrete set values.
 * The flip adapter MUST await options.beforeFlip({flippedAt: canonicalUtcIso, completedAt: epochMs})
 * exactly once at that same config-write boundary, then perform the ONE atomic FILE+hold write
 * using that flippedAt. A rejected hook vetoes the write. No other await/work may intervene.
 * drainReceiptMode:"live" opts in to the A1 BUILT writer; default "probe" only checks recorded calls.
 */
export async function runRollback(request, commandAdapter, { dryRun = true, evidenceKind = "injected-unverified",
  drainReceiptMode = "probe" } = {}) {
  const plan = rollbackPlan(request);
  if (dryRun) return plan; // Does not even inspect or call the adapter.
  requireThat(typeof commandAdapter === "function", "explicit execution needs an injected command adapter");
  const audit = [], drainReceiptWrites = [];
  let phase = "drain-receipt", writeNatsDrainReceipt, drainStates;
  let operationStopped = false;
  const live = drainReceiptMode === "live";
  const call = async (kind, payload = {}, timeoutMs = 30000, hooks = {}) => {
    const action = structuredClone({ kind, host: request.host, runtimeIds: [...request.runtimeIds], ...payload });
    // Only these semantic actions exist. There is no arbitrary command, deletion, purge or ack hook.
    requireThat(["pause-admission", "drain", "write-nats-drain-receipt", "flip-to-file",
      "reconcile-command-ids", "retain-audit"].includes(kind), "forbidden adapter action");
    const controller = new AbortController();
    let timer;
    const entry = { phase, action, status: "in-flight" };
    // A compound flip is appended on completion, after its pre-commit writer calls.
    // Other entries are appended before invocation, retaining uncertain operations in the audit.
    if (kind !== "flip-to-file") audit.push(entry);
    try {
      const result = await Promise.race([
        Promise.resolve().then(() => {
          if (operationStopped) throw drainBlocked("rollback observation stopped; no late receipt writes permitted");
          if (kind === "write-nats-drain-receipt" && live) {
            writeNatsDrainReceipt(action.storeDir, structuredClone(action.receipt));
            return { apiInvoked: true, persistence: "built-writer-returned" };
          }
          return commandAdapter(structuredClone(action), { signal: controller.signal, ...hooks });
        }),
        new Promise((_, reject) => { timer = setTimeout(() => { operationStopped = true; controller.abort(); reject(new Error(`${kind} observation timed out; operation may have persisted`)); }, timeoutMs); }),
      ]);
      entry.status = "received"; entry.result = result;
      return result;
    } catch (error) { entry.status = "uncertain"; throw error; }
    finally { clearTimeout(timer); if (kind === "flip-to-file") audit.push(entry); }
  };
  try {
    requireThat(["live", "probe"].includes(drainReceiptMode), "drainReceiptMode must be live or probe");
    requireThat(!live || (!recordedExecutors.has(commandAdapter) && !evidenceKind.includes("recorded")),
      "recorded replay cannot opt into live host custody writes");
    if (recordedExecutors.has(commandAdapter)) evidenceKind = "recorded-probe-only";
    const stores = validateDrainStores(request);
    phase = "pause";
    const paused = await call("pause-admission");
    requireThat(paused?.paused === true && paused.spawnsRefused === true && paused.receiversDraining === true &&
      paused.code === "FABRIC_TRANSPORT_DRAINING" && paused.notRun === true && paused.persisted === false &&
      sameSet(paused.runtimeIds, request.runtimeIds) && paused.hostInventoryComplete === true,
      "pause receipt must prove all host admission stopped before persistence, without stopping receivers");
    phase = "drain";
    const drain = await call("drain", { waitLimitMs: 120000 }, 125000);
    requireThat(drain && count(drain.elapsedMs) && drain.elapsedMs <= 120000 && Array.isArray(drain.states) &&
      sameSet(drain.states.map((s) => s.runtimeId), request.runtimeIds), "invalid bounded host drain receipt");
    for (const state of drain.states) requireThat(state.stream === `FABRIC_IN_${request.host}` && count(state.pending) && count(state.ackPending),
      "invalid same-host stream/pending/ackPending");
    drainStates = drain.states.map(({ runtimeId, stream, pending, ackPending }) => ({ runtimeId, stream, pending, ackPending }));
    if (!drainStates.every((s) => s.pending === 0 && s.ackPending === 0)) {
      throw blocked("FABRIC_CANARY_ROLLBACK_DRAIN_NOT_EMPTY",
        "BLOCKED: nonzero INFO counters; preserve admission pause and audit. Wait for the owner to drain (or restart it), then retry only by new explicit operator action. If the owner cannot run, a person decides.");
    }
    phase = "drain-receipt";
    const writes = receiptWritePlan(stores, drainStates);
    if (live) {
      try {
        const module = await import(new URL(`../../${DRAIN_WRITER_MODULE}`, import.meta.url).href);
        requireThat(typeof module.writeNatsDrainReceipt === "function", "built module has no writeNatsDrainReceipt export");
        writeNatsDrainReceipt = module.writeNatsDrainReceipt;
      } catch (error) {
        throw blocked("FABRIC_CANARY_ROLLBACK_DRAIN_RECEIPT_WRITER_UNAVAILABLE",
          `BLOCKED: A1 built ${DRAIN_WRITER_MODULE} / ${DRAIN_WRITER_API} unavailable: ${error.message}`);
      }
    }
    let hookEntered = false, hookFinished = false, hookViolated = false, boundaryMillis;
    const liveFlipNotBefore = live ? Date.now() : undefined;
    const beforeFlip = async ({ flippedAt, completedAt } = {}) => {
      if (operationStopped || hookEntered) {
        hookViolated = true;
        throw drainBlocked("beforeFlip must run exactly once before the atomic config write");
      }
      hookEntered = true;
      phase = "drain-receipt";
      boundaryMillis = utcMillis(flippedAt);
      const observedAt = live ? Date.now() : undefined;
      if (!epoch(boundaryMillis) || !epoch(completedAt) || completedAt < boundaryMillis ||
        (live && (boundaryMillis < liveFlipNotBefore || boundaryMillis > observedAt || completedAt > observedAt))) {
        throw drainBlocked("receipt times must be actual same-clock epoch ms: flippedAt <= completedAt; no stale/pre-call or future live timestamps");
      }
      for (const write of writes) {
        const receipt = { ...write.receipt, flippedAt: boundaryMillis, completedAt };
        const result = await call("write-nats-drain-receipt", { module: DRAIN_WRITER_MODULE, api: DRAIN_WRITER_API,
          storeDir: write.storeDir, receipt });
        if (!isDeepStrictEqual(result, { apiInvoked: true, persistence: live ? "built-writer-returned" : "recorded-unverified" })) {
          throw drainBlocked("writer receipt is unverified; probe mode cannot claim native persistence");
        }
        drainReceiptWrites.push({ storeDir: write.storeDir, receipt, persistence: result.persistence });
      }
      if (operationStopped || hookViolated) throw drainBlocked("receipt boundary stopped or invoked more than once; flip is vetoed");
      hookFinished = true;
      phase = "flip";
    };
    phase = "flip";
    const flip = await call("flip-to-file", { transport: "file", reloadAll: true, retainBridge: true,
      hostConfigWrite: fileConfigWrite(), drainReceiptWrite: { module: DRAIN_WRITER_MODULE, api: DRAIN_WRITER_API,
        timing: "before-atomic-host-config-write", writes } }, 120000, { beforeFlip });
    requireThat(hookFinished && !hookViolated, "flip must await the pre-flip built-writer hook exactly once; no drain receipt means no verified flip");
    validateFileConfigWrite(flip?.hostConfigWrite);
    requireThat(utcMillis(flip.hostConfigWrite.flippedAt) === boundaryMillis,
      "drain receipt flippedAt must equal the actual atomic FILE+hold config flippedAt");
    requireThat(flip?.hostTransport === "file" && sameSet(flip.fileRuntimeIds, request.runtimeIds) && flip.oldNatsWritersStopped === true &&
      flip.bridgeUp === true && flip.filePresenceMirrored === true && flip.eventTopicsUnaffected === true && flip.fileAdmissionReady === true &&
      flip.notDelivered !== true && flip.notRun !== true && flip.code !== "CONTROL_NOT_DELIVERED",
      "flip must prove a host-wide reload with the hold, old-writer stop and retained event bridge before reconciliation; no not-delivered/not-run assertion");
    phase = "reconcile";
    const reconciliation = await call("reconcile-command-ids", { commands: [],
      preserveControlSeen: true, maxSameKeyRetries: 0, retainCompletions: true }, 120000);
    requireThat(reconciliation?.controlSeenStoreUnchanged === true && reconciliation.completionJournalRetained === true &&
      reconciliation.completionJournalUnchanged === true && !Object.hasOwn(reconciliation, "completionResendCarrier") &&
      Array.isArray(reconciliation.results) && reconciliation.results.length === 0,
      "E20 reconciliation requires empty inventory/results; native completion journal stays unchanged, with no resend carrier");
    phase = "retain";
    const retained = await call("retain-audit");
    requireThat(retained?.streamsKept === true && retained.consumersKept === true && retained.fabricAdvKept === true &&
      retained.controlSeenKept === true && retained.nativeCustodyKept === true && retained.deletedOrPurged === false,
      "all audit and native custody surfaces must be kept");
    return { schema: "fabric-canary-rollback-report/v1", verdict: "reconciled",
      evidenceKind, e2eProven: false, releaseProven: false, drainReceiptMode, drainReceiptWrites,
      request, fullyDrained: true, drainStates, dispositions: [], reconciliation, audit,
      admission: "file admission ready only after old NATS writers stopped; re-enable NATS is a separate operator-controlled rollout" };
  } catch (error) {
    operationStopped = true;
    return { schema: "fabric-canary-rollback-report/v1", verdict: "blocked", evidenceKind, e2eProven: false,
      releaseProven: false, drainReceiptMode, drainReceiptWrites,
      code: error.rollbackCode ?? (phase === "flip" ? "FABRIC_CANARY_ROLLBACK_FLIP_UNVERIFIED" :
        phase === "drain-receipt" ? "FABRIC_CANARY_ROLLBACK_DRAIN_RECEIPT_UNVERIFIED" : "FABRIC_CANARY_ROLLBACK_BLOCKED"),
      outcome: "outcome-unknown", phase, request, drainStates, dispositions: [], audit, error: error.message,
      recovery: "Stop here. Preserve receipts and ids; independently inspect uncertain actions before any continuation. No auto-resume, retry, unpause or cleanup." };
  }
}

async function main(argv) {
  if (argv.includes("--help")) {
    console.log("Usage: node scripts/canary/rollback.mjs --host ryzen2|ryzen4 --runtimes ID,ID [--dry-run]\n       node scripts/canary/rollback.mjs --replay recorded-calls.json"); return;
  }
  let host, runtimeIds, replay, dryRun = false;
  const seen = new Set();
  for (let i = 0; i < argv.length; i++) {
    const flag = argv[i]; requireThat(!seen.has(flag), `duplicate flag ${flag}`); seen.add(flag);
    if (flag === "--dry-run") dryRun = true;
    else if (["--host", "--runtimes", "--replay"].includes(flag)) {
      const value = argv[++i]; requireThat(value && !value.startsWith("--"), `${flag} needs a value`);
      if (flag === "--host") host = value; else if (flag === "--runtimes") runtimeIds = value.split(","); else replay = value;
    } else throw new Error(`unknown flag ${flag}; this CLI has no live execution mode`);
  }
  let report;
  if (replay) {
    requireThat(!host && !runtimeIds && !dryRun, "--replay cannot be combined with plan options");
    const recorded = JSON.parse(await readFile(replay, "utf8"));
    requireThat(recorded.schema === "fabric-canary-rollback-calls/v1", "invalid recorded-call schema");
    const adapter = recordedAdapter(recorded.calls);
    report = await runRollback(recorded.request, adapter.execute, { dryRun: false, evidenceKind: "recorded-probe-only" });
    if (report.verdict !== "blocked") adapter.finish();
  } else report = rollbackPlan({ host, runtimeIds });
  console.log(JSON.stringify(report, null, 2));
  if (["blocked", "needs-operator"].includes(report.verdict)) process.exitCode = 1;
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { await main(process.argv.slice(2)); }
  catch (error) { console.log(JSON.stringify({ verdict: "invalid", error: error.message })); process.exitCode = 2; }
}
