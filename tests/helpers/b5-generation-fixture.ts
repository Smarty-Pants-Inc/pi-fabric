import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  admitBoundScriptedInput, bindInputAdmission, commitRecoveryInputDelivery, initializeNewRecoverySession, readRecoverySession,
  recordInputOutcome, recoveryHash, releaseRecoveryInputAfterJoin, selectRecoveryInput,
  type RecoveryAdmissionOwner,
} from "../../src/recovery/input-admission.js";
import { createRecoveryMaintenanceAdapter, type RepairRef } from "../../src/recovery/maintenance-admission.js";
import { seedIncompleteRecoveryHistory } from "./b5-c2-seed.js";

// B5-only trust/data-safety fixture: real ledger fsync/rename/reopen; native receipts
// and settlement are boundary fixtures, not a native/B4/B6 producer qualification.
export const B5_HASH = recoveryHash("retained native/B6 fixture bytes");
export function b5GenerationFixture(cleanups: Array<() => void>, complete = true) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "b5-generation-"));
  const sessionId = `session-${path.basename(root)}`;
  const deadlineAt = Date.now() + 120_000;
  let boot = 0;
  let unbind = () => {};
  let nativeAdmissions = 0;
  const mainAgent = { id: `session:${sessionId}`, local: true, sessionId,
    deliverAgent: (): never => { nativeAdmissions++; throw new Error("REQUIRED must not call native admission"); } };
  const bind = (initial = false) => {
    const owner: RecoveryAdmissionOwner = {
      epoch: { processIncarnationId: `process-${boot}`, bindingId: `binding-${boot}`, epoch: 0 },
      podId: `pod-${boot}`, bootId: `boot-${boot}`, historyComplete: complete,
      assertCurrent() {}, reconsiderInput() {},
      validateRepair(ref) { if (ref.repairReceiptSha256 !== B5_HASH) throw new Error("No native fixture receipt"); },
    };
    if (initial) {
      if (complete) initializeNewRecoverySession({ mode: "NEW", root, sessionId, recovery: owner, assertNew() {} });
      else seedIncompleteRecoveryHistory(root, sessionId);
    }
    unbind = bindInputAdmission({ root, sessionId, mainAgent, recovery: owner });
    return owner;
  };
  let owner = bind(true);
  cleanups.push(() => { unbind(); });
  const snapshot = () => readRecoverySession(sessionId);
  const admit = (inputId = "original") => admitBoundScriptedInput({ sessionId, request: {
    from: { id: "driver-K", name: "Driver", kind: "main" }, message: ` exact original bytes ${inputId}\n`,
    delivery: "steer", priority: "interrupt", triggerTurn: true,
    data: { scriptedInput: { version: 1, inputId, sessionId, deadlineAt }, retained: { key: "K", bytes: "unchanged" } },
  } });
  const select = async (inputId = "original") => {
    const selected = await selectRecoveryInput({ sessionId, originalInputId: inputId, epoch: owner.epoch, expectedRevision: snapshot().revision });
    if (!selected.selected) throw new Error(selected.code);
    return selected.input;
  };
  const outcome = (kind: "lifecycle-interrupt" | "user-cancel" = "lifecycle-interrupt") => recordInputOutcome({
    sessionId, originalInputId: "original", requestId: `${kind}-${boot}`, kind,
    at: Date.now(), bootId: owner.bootId, deadlineMs: Date.now() + 10_000,
  });
  const joinedOriginal = async () => {
    const receipt = admit();
    const input = await select();
    await commitRecoveryInputDelivery(input, { entryId: "original-entry", bytesSha256: B5_HASH, seq: 1, executionBranchEntryId: "anchor-A" });
    await outcome();
    await releaseRecoveryInputAfterJoin(input, Promise.resolve(), async () => {});
    return { receipt, input };
  };
  const maintenance = createRecoveryMaintenanceAdapter();
  const maintenanceRequest = (repair: RepairRef, requestId = "retry") => ({
    version: 1 as const, admissionKind: "maintenance" as const, sessionId, requestId, epoch: owner.epoch,
    operation: { kind: "auto-compact-retry" as const, retryAnchor: "original" },
    baseLeafId: "anchor-A", baseHistorySha256: B5_HASH, viewSha256: B5_HASH,
    issuedAtMs: Date.now(), deadlineMs: deadlineAt, repair,
  });
  const finishMaintenance = (requestId = "retry") => maintenance.reconcileAfterJoin({ sessionId, requestId }, owner.epoch, {
    deadlineMs: Date.now() + 30_000, joined: Promise.resolve(),
    settle: async (_record, prepare) => {
      // Trusted native/B6 port fixture only; not an assertion that B5 produced repair.
      const plan = { entryId: `prepare-${requestId}`, bytesSha256: B5_HASH, planSha256: B5_HASH };
      const prepared = await prepare(plan); if (!prepared.ok) throw new Error(prepared.code);
      return { outcome: "failed", errorCode: "fixture-overflow", checkpointEntryId: `checkpoint-${requestId}`,
        entries: [{ entryId: plan.entryId, bytesSha256: B5_HASH, seq: 2 }, { entryId: `checkpoint-${requestId}`, bytesSha256: B5_HASH, seq: 3 }] };
    }, reconcile: async () => {},
  });
  return { root, sessionId, deadlineAt, snapshot, admit, select, outcome, joinedOriginal, maintenance, maintenanceRequest, finishMaintenance,
    ledgerFile: path.join(root, "input-admission", "v1", "session", `${recoveryHash(sessionId)}.json`),
    get owner() { return owner; }, get nativeAdmissions() { return nativeAdmissions; },
    reopen() { unbind(); boot++; owner = bind(); },
  };
}
