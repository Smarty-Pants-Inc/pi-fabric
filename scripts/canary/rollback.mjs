#!/usr/bin/env node
// Frozen contract §8.3. The CLI only prints plans or replays JSON receipts.
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";

const requireThat = (test, message) => { if (!test) throw new Error(message); };
const text = (s) => typeof s === "string" && s.length > 0;
const count = (n) => Number.isSafeInteger(n) && n >= 0;
const sameSet = (a, b) => Array.isArray(a) && a.length === b.length && new Set(a).size === a.length && b.every((v) => a.includes(v));

/** @typedef {"queued"|"admitted"|"unknown"|"terminal"} AdmissionClass */
/**
 * @typedef {Object} CommandDisposition
 * @property {string} commandId Carrier-independent dedupe key, never replaced on retry.
 * @property {AdmissionClass} classification
 * @property {"FABRIC_RELAY_DESTINATION_UNAVAILABLE"} code
 * @property {"outcome-unknown"} outcome
 * @property {string} relayId Stable relay:<commandId> key.
 * @property {"same-key-once-after-flip"} retry
 */

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
      { phase: "drain", action: "drain", waitLimitMs: 120000, require: "drainState pending=0, ackPending=0 on EVERY runtime, or classify after bounded timeout" },
      { phase: "classify", action: "classify-retained", require: "complete operator MSG.GET inventory, joined to carrier-independent control-seen; no ack or mutation",
        classes: { queued: "no claim", admitted: "claim with outcome", unknown: "claim without outcome", terminal: "claim with terminal outcome" } },
      { phase: "disposition", action: "relay-outcome-unknown", require: "one durable idempotent relay:<commandId> per remaining command, including admitted and terminal; retain ACK envelopes" },
      { phase: "flip", action: "flip-to-file", require: "host-wide config flip, reload ALL runtimes, confirm old NATS writers stopped before file admission is ready; bridge stays up and resumes file presence mirrors" },
      { phase: "reconcile", action: "reconcile-command-ids", require: "same commandId/control-seen store; at most one same-key retry; never re-run an unresolved claim; native same-root completion journal retained unchanged; no completion carrier replay" },
      { phase: "retain", action: "retain-audit", require: "keep streams, consumers, FABRIC_ADV, control-seen and ALL native custody/consumption journals" },
    ],
    forbidden: ["delete", "purge", "ack remaining messages as cleanup", "new-id retry", "automatic TTL takeover", "stop event bridge",
      "completion carrier replay", "completion control-seen claim"],
    note: "This is a per-host control/presence rollback, not fleet cutover or file-surface retirement. E4 completions have no transport rollback; native envelopes, attempts, receipts, archives and claims stay unchanged.",
  };
}

/** Exact recorded-call adapter: no dynamic code, shell, broker access, or writes. */
export function recordedAdapter(calls) {
  requireThat(Array.isArray(calls), "calls must be an array");
  let index = 0;
  const execute = async (action) => {
    const call = calls[index++];
    requireThat(call && isDeepStrictEqual(call.action, action), `recorded call ${index} does not match ${action.kind}`);
    requireThat(Object.hasOwn(call, "result"), `recorded call ${index} has no result`);
    return structuredClone(call.result);
  };
  return { execute, finish: () => requireThat(index === calls.length, "unused recorded calls; transcript is not an exact replay") };
}

/**
 * Sole side-effect boundary: a trusted, explicitly injected adapter receives structured actions.
 * It must respect AbortSignal and own finite host/CLI deadlines. Aborting observation cannot undo
 * a persisted action; on error this function stops and returns its partial audit, never auto-retries.
 */
export async function runRollback(request, commandAdapter, { dryRun = true, evidenceKind = "injected-unverified" } = {}) {
  const plan = rollbackPlan(request);
  if (dryRun) return plan; // Does not even inspect or call the adapter.
  requireThat(typeof commandAdapter === "function", "explicit execution needs an injected command adapter");
  const audit = [], dispositions = [], retainedAcks = [];
  let phase = "pause";
  const call = async (kind, payload = {}, timeoutMs = 30000) => {
    const action = { kind, host: request.host, runtimeIds: [...request.runtimeIds], ...payload };
    // Only these semantic actions exist. There is no arbitrary command, deletion, purge or ack hook.
    requireThat(["pause-admission", "drain", "classify-retained", "relay-outcome-unknown", "flip-to-file",
      "reconcile-command-ids", "retain-audit"].includes(kind), "forbidden adapter action");
    const controller = new AbortController();
    let timer;
    const entry = { phase, action, status: "in-flight" };
    audit.push(entry);
    try {
      const result = await Promise.race([
        Promise.resolve().then(() => commandAdapter(action, { signal: controller.signal })),
        new Promise((_, reject) => { timer = setTimeout(() => { controller.abort(); reject(new Error(`${kind} observation timed out; operation may have persisted`)); }, timeoutMs); }),
      ]);
      entry.status = "received"; entry.result = result;
      return result;
    } catch (error) { entry.status = "uncertain"; throw error; }
    finally { clearTimeout(timer); }
  };
  try {
    const paused = await call("pause-admission");
    requireThat(paused?.paused === true && paused.spawnsRefused === true && paused.receiversDraining === true &&
      paused.code === "FABRIC_TRANSPORT_DRAINING" && paused.notRun === true && paused.persisted === false &&
      sameSet(paused.runtimeIds, request.runtimeIds) && paused.hostInventoryComplete === true,
      "pause receipt must prove all host admission stopped before persistence, without stopping receivers");
    phase = "drain";
    const drain = await call("drain", { waitLimitMs: 120000 }, 125000);
    requireThat(drain && count(drain.elapsedMs) && drain.elapsedMs <= 120000 && Array.isArray(drain.states) &&
      sameSet(drain.states.map((s) => s.runtimeId), request.runtimeIds), "invalid bounded host drain receipt");
    for (const state of drain.states) requireThat(count(state.pending) && count(state.ackPending), "invalid pending/ackPending");
    const fullyDrained = drain.states.every((s) => s.pending === 0 && s.ackPending === 0);
    requireThat(fullyDrained || drain.elapsedMs === 120000, "must allow the full two-minute drain before classifying nonzero queues");
    phase = "classify";
    const inventory = await call("classify-retained", { states: drain.states });
    requireThat(inventory?.complete === true && inventory.pausedInventoryStable === true &&
      inventory.controlSeenStoreUnchanged === true && Array.isArray(inventory.messages), "classification needs a complete stable MSG.GET/control-seen inventory");
    const sequences = new Set(), commands = new Map();
    for (const message of inventory.messages) {
      requireThat(message && request.runtimeIds.includes(message.runtimeId) && Number.isSafeInteger(message.sequence) && message.sequence > 0 &&
        ["command", "ack"].includes(message.kind), "invalid retained envelope");
      requireThat(message.operation !== "completion" && !(text(message.commandId) && message.commandId.startsWith("completion:")),
        "E4 completion has no carrier envelope or control-seen claim; completion:<runId> is reserved, not reconciled");
      const sequenceKey = `${message.runtimeId}:${message.sequence}`;
      requireThat(!sequences.has(sequenceKey), "duplicate retained sequence"); sequences.add(sequenceKey);
      if (message.kind === "ack") {
        retainedAcks.push({ runtimeId: message.runtimeId, sequence: message.sequence, disposition: "retained-for-audit" });
        continue;
      }
      requireThat(text(message.commandId) && text(message.replyTo) && ["absent", "outcome", "pending"].includes(message.claim),
        "command must have an explicit claim state and replyTo");
      if (message.claim === "outcome") requireThat(message.outcome !== undefined && message.outcome !== null,
        "admitted command requires its durable stored outcome");
      else requireThat(message.outcome === undefined && message.terminal !== true, "queued/unknown command cannot claim a known outcome");
      const classification = message.claim === "absent" ? "queued" : message.claim === "pending" ? "unknown" : message.terminal === true ? "terminal" : "admitted";
      const value = { commandId: message.commandId, replyTo: message.replyTo, classification,
        ...(message.claim === "outcome" ? { storedOutcome: message.outcome } : {}) };
      if (commands.has(message.commandId)) requireThat(isDeepStrictEqual(commands.get(message.commandId), value), "conflicting duplicate command classification");
      commands.set(message.commandId, value);
    }
    for (const state of drain.states) requireThat(inventory.messages.filter((m) => m.runtimeId === state.runtimeId).length === state.pending + state.ackPending,
      `incomplete retained envelope inventory for ${state.runtimeId}`);
    phase = "disposition";
    for (const command of commands.values()) {
      /** @type {CommandDisposition} */
      const disposition = { commandId: command.commandId, classification: command.classification,
        relayId: `relay:${command.commandId}`, code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", outcome: "outcome-unknown", retry: "same-key-once-after-flip" };
      const receipt = await call("relay-outcome-unknown", { disposition, replyTo: command.replyTo });
      requireThat(receipt?.id === disposition.relayId && receipt.persisted === true &&
        Number.isSafeInteger(receipt.sequence) && receipt.sequence > 0, "relay needs a durable correlated PubAck before flip");
      dispositions.push({ ...disposition, receipt });
    }
    phase = "flip";
    const flip = await call("flip-to-file", { transport: "file", reloadAll: true, retainBridge: true }, 120000);
    requireThat(flip?.hostTransport === "file" && sameSet(flip.fileRuntimeIds, request.runtimeIds) && flip.oldNatsWritersStopped === true &&
      flip.bridgeUp === true && flip.filePresenceMirrored === true && flip.eventTopicsUnaffected === true && flip.fileAdmissionReady === true,
      "flip must prove a host-wide reload, old-writer stop and retained event bridge before reconciliation");
    phase = "reconcile";
    const reconciliation = await call("reconcile-command-ids", { commands: [...commands.values()],
      preserveControlSeen: true, maxSameKeyRetries: 1, retainCompletions: true }, 120000);
    requireThat(reconciliation?.controlSeenStoreUnchanged === true && reconciliation.completionJournalRetained === true &&
      reconciliation.completionJournalUnchanged === true && !Object.hasOwn(reconciliation, "completionResendCarrier") &&
      Array.isArray(reconciliation.results) && sameSet(reconciliation.results.map((r) => r.commandId), [...commands.keys()]),
      "every retained control command needs a reconciliation disposition; native completion journal stays unchanged, with no resend carrier");
    let unresolved = 0;
    for (const result of reconciliation.results) {
      const command = commands.get(result.commandId);
      requireThat(count(result.sameKeyRetries) && result.sameKeyRetries <= 1 && result.commandIdUnchanged === true &&
        result.claimPreserved === true && (result.effectCount === null || (count(result.effectCount) && result.effectCount <= 1)),
        "same-id retry, preserved claim and zero double effects required");
      if (["admitted", "terminal"].includes(command.classification)) {
        requireThat(result.disposition === "stored-outcome" && isDeepStrictEqual(result.outcome, command.storedOutcome) &&
          result.handlerReexecuted === false, "admitted/terminal retry must return the stored outcome, not execute again");
      } else if (command.classification === "unknown") {
        requireThat(result.disposition === "held-unknown" && result.handlerReexecuted === false && result.sameKeyRetries === 0,
          "claim without outcome is held for operator resolution, never re-executed");
      } else requireThat(["admitted-once", "held-queued"].includes(result.disposition), "queued command must be admitted once or explicitly held");
      if (result.disposition.startsWith("held-")) unresolved++;
      else requireThat(result.effectCount !== null && text(result.receiptId), "settled reconciliation needs effect evidence and a receipt");
      if (result.disposition === "admitted-once") requireThat(result.sameKeyRetries === 1 && result.effectCount === 1 && result.handlerReexecuted === false,
        "a queued command may execute once only through its same-key retry");
      if (result.disposition === "held-queued") requireThat(result.sameKeyRetries === 0 && result.handlerReexecuted === false && result.effectCount === 0,
        "a queued hold must prove nonexecution and must not disguise a retry");
    }
    phase = "retain";
    const retained = await call("retain-audit");
    requireThat(retained?.streamsKept === true && retained.consumersKept === true && retained.fabricAdvKept === true &&
      retained.controlSeenKept === true && retained.nativeCustodyKept === true && retained.deletedOrPurged === false,
      "all audit and native custody surfaces must be kept");
    return { schema: "fabric-canary-rollback-report/v1", verdict: unresolved ? "needs-operator" : "reconciled",
      evidenceKind, e2eProven: false, request, fullyDrained, dispositions, retainedAcks, reconciliation, audit,
      admission: "file admission ready only after old NATS writers stopped; re-enable NATS is a separate operator-controlled rollout" };
  } catch (error) {
    return { schema: "fabric-canary-rollback-report/v1", verdict: "blocked", evidenceKind, e2eProven: false,
      phase, request, dispositions, retainedAcks, audit, error: error.message,
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
