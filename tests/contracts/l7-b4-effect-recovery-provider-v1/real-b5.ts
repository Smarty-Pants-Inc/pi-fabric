// Real-B5 target, committed for v6 from the Ch1 composition harness.
// Requires the combined branch's real B5 exports; no production patch or fake B4.
// Real B5 admission/selection/pin/delivery ledger. Native custody and delivery
// descriptors below are probe:native-Pi, NOT native/B6 qualification.
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect } from "vitest";
import * as b5 from "../../../src/recovery/input-admission.js";
import { createReal } from "./real.js";
import { hash, immutable, INPUT, INPUT_LIFETIME_MS, joinedBranch, SESSION,
  type ConformanceFactory, type ConformanceHarness, type RecoveryIdentity } from "./fixtures.js";

export const createRealB5: ConformanceFactory = async scenario => {
  const root = mkdtempSync(join(tmpdir(), "ch1-real-b5-"));
  const now = Date.now;
  const deadlineMs = now() + INPUT_LIFETIME_MS;
  let incarnation = 0;
  let unbind = () => {};
  let h: ConformanceHarness | undefined;
  let identity: RecoveryIdentity;
  const owner = (): b5.RecoveryAdmissionOwner => {
    const current = incarnation;
    return {
      epoch: { processIncarnationId: `probe-process-${current}`, bindingId: `probe-binding-${current}`, epoch: current },
      podId: "probe:native-Pi", bootId: `probe-boot-${current}`, historyComplete: true,
      assertCurrent() { if (current !== incarnation) throw new b5.RecoveryAdmissionError("RECOVERY_EPOCH_REVOKED", "Probe owner was replaced"); },
      reconsiderInput() {},
      validateRepair(ref) {
        if (!h?.provider.resolveRepair) throw new b5.RecoveryAdmissionError("RECOVERY_MAINTENANCE_REPAIR_REQUIRED", "Real B4 resolver absent");
        h.provider.resolveRepair(ref);
      },
    };
  };
  let recovery = owner();
  const bind = () => { unbind = b5.bindInputAdmission({ root, sessionId: SESSION, recovery, mainAgent: {
    id: `session:${SESSION}`, sessionId: SESSION, local: true,
    deliverAgent(): never { throw new Error("REQUIRED admission must not directly deliver native input"); },
  } }); };
  try {
    b5.initializeNewRecoverySession({ mode: "NEW", root, sessionId: SESSION, recovery, assertNew() {} });
    bind();
    b5.admitBoundScriptedInput({ sessionId: SESSION, request: {
      from: { id: "ch1-driver", name: "Ch1", kind: "main" }, message: "original input", delivery: "followUp", triggerTurn: true,
      data: { scriptedInput: { version: 1, inputId: INPUT, sessionId: SESSION, deadlineAt: deadlineMs } },
    } });
    const result = await b5.selectRecoveryInput({ sessionId: SESSION, originalInputId: INPUT, epoch: recovery.epoch,
      expectedRevision: b5.readRecoverySession(SESSION).revision });
    if (!result.selected) throw new Error(result.code);
    const selected = result.input;
    expect(selected.deliveryId).toBe(selected.admissionId);
    expect(selected.deadlineMs).toBe(deadlineMs);
    const generation = await b5.ensureRecoveryGeneration(SESSION, selected.originalInputId);
    const deliveredIdentity = { acceptedInput: {
      sessionId: selected.sessionId, originalInputId: selected.originalInputId, admissionId: selected.admissionId,
      deliveryId: selected.deliveryId, text: selected.text, deadlineMs: selected.deadlineMs,
      payloadSha256: hash(selected.text), kind: "initial" as const, consumption: { kind: "round" as const },
    }, executionBranchEntryId: "A" };
    // Use the same native-shaped carrier as the branch/scan probe, in native
    // property order with exactly one LF; B5 retains the hash of these bytes.
    const carrier = joinedBranch("J", deliveredIdentity)[0]!;
    const rawCarrier = Buffer.from(JSON.stringify(carrier) + "\n", "utf8");
    await b5.commitRecoveryInputDelivery(selected, { entryId: carrier.id, executionBranchEntryId: deliveredIdentity.executionBranchEntryId,
      seq: 1, bytesSha256: hash(rawCarrier) });
    const outcome = await b5.recordInputOutcome({ sessionId: SESSION, originalInputId: INPUT, requestId: "joined-interruption",
      kind: "lifecycle-interrupt", at: now(), bootId: recovery.bootId, deadlineMs: Math.min(deadlineMs, now() + 10_000) });
    if (!outcome.durable) throw new Error(outcome.reason);
    await b5.releaseRecoveryInputAfterJoin(selected, Promise.resolve(), async () => {});
    const retained = b5.readRecoverySession(SESSION);
    const original = retained.inputs.find(input => input.admissionId === selected.admissionId)!;
    expect(original.delivery).toMatchObject({ entryId: carrier.id, executionBranchEntryId: deliveredIdentity.executionBranchEntryId,
      seq: 1, bytesSha256: hash(rawCarrier) });
    expect(retained.recoveryGenerationPins).toContainEqual({ originalInputId: selected.originalInputId,
      admissionId: selected.admissionId, recoveryGeneration: generation });
    identity = immutable({ ...deliveredIdentity,
      executionBranchEntryId: original.delivery!.executionBranchEntryId, recoveryGeneration: generation });
    // No B4 wrappers or fake repair implementations. Only fixture accessors change.
    h = await createReal(scenario, { recoveryIdentity: () => identity });
    const reopen = h.reopen.bind(h);
    h.reopen = async () => {
      unbind(); incarnation++; recovery = owner(); bind();
      const pin = await b5.ensureRecoveryGeneration(SESSION, selected.originalInputId);
      const original = b5.readRecoverySession(SESSION).inputs.find(input => input.admissionId === selected.admissionId)!;
      expect(pin).toBe(identity.recoveryGeneration);
      expect(original.admissionId).toBe(identity.acceptedInput.admissionId);
      expect(original.deadlineAt).toBe(identity.acceptedInput.deadlineMs);
      expect(original.delivery!.executionBranchEntryId).toBe(identity.executionBranchEntryId);
      await reopen();
    };
    const dispose = h.dispose.bind(h);
    h.dispose = async () => { try { await dispose(); } finally { unbind(); rmSync(root, { recursive: true, force: true }); } };
    console.log("CH1_REAL_B5", JSON.stringify({ admissionId: selected.admissionId, deliveryId: selected.deliveryId,
      recoveryGeneration: generation, deadlineMs, clockAtAdmission: deadlineMs - INPUT_LIFETIME_MS }));
    return h;
  } catch (error) {
    try { await h?.dispose(); } finally { unbind(); rmSync(root, { recursive: true, force: true }); }
    throw error;
  }
};
