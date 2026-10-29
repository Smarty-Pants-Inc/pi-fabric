// Real B5/filesystem adapter. Native receipts/custody below are explicit test fixtures.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { vi } from "vitest";
import * as input from "../../../src/recovery/input-admission.js";
import { createRecoveryMaintenanceAdapter } from "../../../src/recovery/maintenance-admission.js";
import { UNKNOWN_ACK_MESSAGE, type AdmissionHarness } from "./types.js";

export function createRealB5Harness(): AdmissionHarness {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "l7-b5-contract-"));
  const sessionId = path.basename(root), originalInputId = "original";
  const deadlineMs = Date.now() + 120_000;
  const digest = input.recoveryHash("test-only native receipt bytes");
  const ledger = path.join(root, "input-admission", "v1", "session", `${input.recoveryHash(sessionId)}.json`);
  let required = false, incarnation = 0, nativeDeliveries = 0, bound = false;
  let unbind = () => {};
  const owner = (): input.RecoveryAdmissionOwner => {
    const identity = incarnation;
    return {
      epoch: { processIncarnationId: `process-${identity}`, bindingId: `binding-${identity}`, epoch: identity },
      podId: "original-pod", bootId: `boot-${identity}`, historyComplete: true,
      assertCurrent() {
        if (identity !== incarnation) throw new input.RecoveryAdmissionError("RECOVERY_EPOCH_REVOKED", "Fixture custody revoked");
      },
      reconsiderInput() {},
      validateRepair(ref) {
        if (ref.repairReceiptSha256 !== digest) throw new input.RecoveryAdmissionError("RECOVERY_CONFLICT", "Not the fixture receipt");
      },
    };
  };
  let recovery = owner();
  const initialization: input.NewRecoverySessionInitialization = { mode: "NEW", root, sessionId, recovery,
    assertNew() {
      if (required) throw new input.RecoveryAdmissionError("RECOVERY_CONFLICT", "Authoritative fixture identity is REQUIRED");
    },
  };
  const unbindRequired = () => { unbind(); unbind = () => {}; bound = false; };
  const bindRequired = () => {
    required = true;
    unbind = input.bindInputAdmission({ root, sessionId, recovery, mainAgent: {
      id: `session:${sessionId}`, sessionId, local: true,
      deliverAgent(): never { nativeDeliveries++; throw new Error("REQUIRED B5 must not directly deliver native input"); },
    } });
    bound = true;
  };
  // Observe raw bytes/identity, not a successful authoritative read. Public operations
  // still go through the real checksummed file + namespace barriers on every call.
  const storage = (): string => {
    const walk = (dir: string): unknown[] => fs.readdirSync(dir).sort().map(name => {
      const file = path.join(dir, name), stat = fs.lstatSync(file);
      return [path.relative(root, file), stat.ino, stat.isDirectory() ? walk(file) : fs.readFileSync(file, "utf8")];
    });
    return JSON.stringify(walk(root));
  };
  return {
    admission: { ...input.createRecoveryInputAdapter(),
      initializeNewRecoverySession: input.initializeNewRecoverySession,
      recordRecoveryRepair: input.recordRecoveryRepair,
      maintenance: createRecoveryMaintenanceAdapter(),
    },
    sessionId, originalInputId, deadlineMs, initialization,
    requireIdentity() { required = true; }, bindRequired, unbind: unbindRequired,
    rebindRequired() { unbindRequired(); incarnation++; recovery = owner(); bindRequired(); },
    async seedJoinedOriginal() {
      if (!bound) throw new Error("Bind REQUIRED before fixture admission");
      input.admitBoundScriptedInput({ sessionId, request: {
        from: { id: "driver-K", name: "Driver", kind: "main" },
        message: " exact original bytes K\n", delivery: "followUp", triggerTurn: true,
        data: { scriptedInput: { version: 1, inputId: originalInputId, sessionId, deadlineAt: deadlineMs } },
      } });
      const selected = await input.selectRecoveryInput({ sessionId, originalInputId, epoch: recovery.epoch,
        expectedRevision: input.readRecoverySession(sessionId).revision });
      if (!selected.selected) throw new Error(selected.code);
      await input.commitRecoveryInputDelivery(selected.input, {
        entryId: "original-entry", bytesSha256: digest, seq: 1, executionBranchEntryId: "anchor-A",
      });
      const outcome = await input.recordInputOutcome({ sessionId, originalInputId, requestId: "joined-interruption",
        kind: "lifecycle-interrupt", at: Date.now(), bootId: recovery.bootId, deadlineMs: Math.min(deadlineMs, Date.now() + 10_000) });
      if (!outcome.durable) throw new Error(outcome.reason);
      await input.releaseRecoveryInputAfterJoin(selected.input, Promise.resolve(), async () => {});
    },
    repairFor(recoveryGeneration) { return { originalInputId, recoveryGeneration,
      executionBranchEntryId: "anchor-A", repairReceiptSha256: digest }; },
    observe() {
      const state = fs.existsSync(ledger)
        ? (JSON.parse(fs.readFileSync(ledger, "utf8")) as { payload: input.RecoverySessionLedger }).payload : undefined;
      return { revision: state?.revision, pins: state?.recoveryGenerationPins ?? [],
        continuationIds: state?.inputs.filter(i => i.kind === "continuation").map(i => i.admissionId) ?? [],
        repairs: state?.repairs ?? [], storage: storage(), nativeDeliveries };
    },
    removeLedger() { fs.unlinkSync(ledger); },
    async withLostWriteAck(write) {
      const rename = fs.renameSync;
      let injected = false;
      const spy = vi.spyOn(fs, "renameSync").mockImplementation((...args) => {
        rename(...args);
        if (!injected && String(args[1]) === ledger) { injected = true; throw new Error(UNKNOWN_ACK_MESSAGE); }
      });
      try {
        const value = await write();
        if (!injected) throw new Error("Lost-ACK scenario did not reach ledger publish");
        return value;
      } finally { spy.mockRestore(); }
    },
    close() { unbindRequired(); fs.rmSync(root, { recursive: true, force: true }); },
  };
}
