import type { CertifiedEffectRecordPort } from "./extension.js";
import type { RecoveryOwner } from "./contracts.js";
import { EffectContractError } from "./contracts.js";
import type { B4EffectRecoveryProviderV1, NativeAcceptedRecoveryInput, RepairRef } from "./repair-contracts.js";
import type { BoundRepairOriginal, MaintenanceRepairEngine, RepairNativeReader } from "./maintenance-repair.js";
import { canonicalEncode, sha256Bytes } from "./slots.js";
import { readRecoverySession, recordRecoveryRepair, recoveryOwner, RecoveryAdmissionError } from "../recovery/input-admission.js";
import type { RecoveryAdmissionOwner, RecoveryIo, RecoverySessionLedger } from "../recovery/input-admission.js";
import type { RecoveryToolResultRead } from "../recovery/l7-interfaces.js";

// Stable opt-in entry exports both real factories. No root-barrel activation.
export { createMaintenanceRepairEngine } from "./maintenance-repair.js";
export type { MaintenanceRepairEngineOptions, RepairNativeReader, BoundRepairOriginal } from "./maintenance-repair.js";

export interface MaintenanceRepairHostOptions {
  readonly owner: RecoveryOwner;
  readonly engine: MaintenanceRepairEngine;
  readonly recordPort: CertifiedEffectRecordPort;
  readonly native: RepairNativeReader;
  readonly admissionOwner: RecoveryAdmissionOwner;
  readonly io?: RecoveryIo;
}
const required = (message: string): never => { throw new EffectContractError("RECOVERY_MAINTENANCE_REPAIR_REQUIRED", message); };
const conflict = (message: string): never => { throw new RecoveryAdmissionError("RECOVERY_CONFLICT", message); };
const incomplete = (message: string): never => { throw new RecoveryAdmissionError("RECOVERY_HISTORY_INCOMPLETE", message); };
const object = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);
const text = (v: unknown): v is string => typeof v === "string" && v.length > 0;
const same = (a: unknown, b: unknown): boolean => canonicalEncode(a) === canonicalEncode(b);
const terminal = (state: RecoverySessionLedger, id: string): boolean => state.outcomes.some(row => row.originalInputId === id && row.kind !== "lifecycle-interrupt");
/** Approved Q2 open-original predicate: a genuinely open, delivered current
 * original may produce settlements:[]; no-original clean state creates no receipt. */
const canCreateForOpenOriginal = (state: RecoverySessionLedger, id: string): boolean => state.currentInputId === id && !terminal(state, id);
const inputCopy = (input: Readonly<NativeAcceptedRecoveryInput>, sessionId: string): Readonly<NativeAcceptedRecoveryInput> => {
  try {
    const encoded = canonicalEncode(input);
    if (!object(input) || input.sessionId !== sessionId || !text(input.originalInputId)) return conflict("Repair session/original identity mismatch");
    return JSON.parse(encoded) as NativeAcceptedRecoveryInput;
  } catch (error) {
    if (error instanceof RecoveryAdmissionError) throw error;
    return conflict("Native accepted occurrence must be lossless plain JSON");
  }
};
const authenticate = (state: RecoverySessionLedger, input: Readonly<NativeAcceptedRecoveryInput>): BoundRepairOriginal => {
  if (!state.complete) return incomplete("Complete original/outcome history is required for repair");
  const original = state.inputs.find(row => row.kind === "round" && row.originalInputId === input.originalInputId);
  if (!original) return incomplete("Original is absent from actual B5 accepted history");
  const keys = ["sessionId", "originalInputId", "admissionId", "deliveryId", "kind", "text", "payloadSha256", "deadlineMs", "consumption"];
  if (Reflect.ownKeys(input).length !== keys.length || !Object.keys(input).every(key => keys.includes(key))) return conflict("Malformed native accepted occurrence");
  const occurrence = state.inputs.find(row => row.originalInputId === input.originalInputId && row.admissionId === input.admissionId);
  if (!occurrence || input.deliveryId !== occurrence.admissionId || input.kind !== (occurrence.kind === "round" ? "initial" : "recovery") ||
      input.text !== occurrence.round.message || input.payloadSha256 !== sha256Bytes(occurrence.round.message) ||
      input.deadlineMs !== original.deadlineAt || !object(input.consumption) || !same(input.consumption, { kind: "round" })) {
    return conflict("Native occurrence/text/hash/kind/deadline/consumption differs from retained B5 admission");
  }
  if (state.reservation?.kind === "input") return required("Actual B5 input reservation has not completed its join/release edge");
  if (!original.delivery || !occurrence.delivery) return required("Original and selected occurrence require genuine committed native deliveries");
  // Read the NAMED original/admission pin only. B5 remains its sole enforcer;
  // this host never allocates, remints, seeds or compares different originals.
  const pin = state.recoveryGenerationPins.find(row => row.originalInputId === input.originalInputId && row.admissionId === original.admissionId);
  if (!pin) return required("Named original has no retained B5 recovery generation pin");
  return {
    input, originalAdmissionId: original.admissionId, originalPayloadSha256: sha256Bytes(original.round.message),
    originalDeadlineMs: original.deadlineAt, originalCarrierEntryId: original.delivery.entryId,
    executionBranchEntryId: original.delivery.executionBranchEntryId, recoveryGeneration: pin.recoveryGeneration,
    carriers: state.inputs.flatMap(row => row.delivery ? [{ entryId: row.delivery.entryId, deliveryId: row.admissionId,
      bytesSha256: row.delivery.bytesSha256, seq: row.delivery.seq }] : []),
  };
};

/** B4-authored complete four-method provider over the real B5 exports. Integration
 * order: engine -> real owner with `validateRepair: ref => { engine.resolveRepair(ref); }`
 * -> bindInputAdmission -> host. Reopen prepares retained refs asynchronously on
 * that engine before B5 invokes its synchronous validator. Disposal/rebind must
 * call engine.invalidateRepairResolution and unbind actual B5. */
export function createMaintenanceRepairHost(options: MaintenanceRepairHostOptions): B4EffectRecoveryProviderV1 {
  const { owner, engine, recordPort, native, admissionOwner, io } = options;
  const sessionId = owner.logicalSessionId;
  const assertBound = (): void => {
    try {
      if (owner.logicalSessionId !== sessionId || recoveryOwner(sessionId) !== admissionOwner) {
        throw new RecoveryAdmissionError("RECOVERY_EPOCH_REVOKED", "Repair host does not own the actual bound B5/native session");
      }
    } catch (error) { engine.invalidateRepairResolution(); throw error; }
  };
  const readState = (): RecoverySessionLedger => {
    assertBound();
    try { return readRecoverySession(sessionId, io); }
    catch (error) {
      if (!(error instanceof RecoveryAdmissionError) || !["RECOVERY_ABORTED", "RECOVERY_DEADLINE", "RECOVERY_CONFLICT"].includes(error.code)) engine.invalidateRepairResolution();
      throw error;
    }
  };
  const checkJoined = (ref: RepairRef, expectedLeaf: string): void => {
    assertBound();
    const view = engine.resolveRepair(ref);
    let leaf: string | null, history: string;
    try {
      leaf = native.getLeafId();
      const branch = native.getBranch(expectedLeaf);
      // NEVER canonical-clone native objects before computing this hash.
      for (const entry of branch) canonicalEncode(entry);
      history = sha256Bytes(JSON.stringify(branch.map(entry => [entry.id, sha256Bytes(JSON.stringify(entry) + "\n")])));
    } catch (error) { engine.invalidateRepairResolution(); throw error; }
    if (leaf !== expectedLeaf || view.joinedLeafEntryId !== expectedLeaf || view.joinedHistorySha256 !== history) {
      return required("Current joined native J/history changed; retained historical repair is not fresh permission");
    }
  };
  const provider: B4EffectRecoveryProviderV1 = {
    settlePendingIntents: (session: string, generation: string) => recordPort.settlePendingIntents(session, generation),
    // The frozen legacy ABI names the native message type also for the labelled
    // adapter-original path. Preserve the actual timestamp-free C35 bytes.
    readToolResult: effect => recordPort.readToolResult(effect) as Promise<RecoveryToolResultRead>,
    resolveRepair(ref) { assertBound(); return engine.resolveRepair(ref); },
    async repairMaintenance(supplied, executionLeafId) {
      const input = inputCopy(supplied, sessionId);
      const basis = await engine.readRepairBasis(input.originalInputId);
      const state = readState();
      const old = state.repairs.find(row => row.originalInputId === input.originalInputId);
      if (basis.records.length === 0 && state.currentInputId !== input.originalInputId && !old) {
        return conflict("Original has no B4 association and is not the actual B5 current original");
      }
      // Ordered ahead of occurrence/pin comparisons, independently of other originals.
      if (basis.records.some(row => !row.transitions.some(transition => transition.state === "RESULT" &&
          (transition.outcome === "committed" || transition.outcome === "not-committed")))) {
        return required("This original still has unresolved issued effects");
      }
      const binding = authenticate(state, input);
      if (!old && !canCreateForOpenOriginal(state, input.originalInputId)) return conflict("A terminal/noncurrent original cannot create a fresh repair");
      if (!text(executionLeafId)) return required("An actual non-null joined native leaf is required");
      const ref = await engine.certifyRepair(binding, executionLeafId);
      checkJoined(ref, executionLeafId);
      // No store lock is held. Actual B5 checks the pin, then calls the actual
      // void validator, including on its retained exact-retry path.
      try { await recordRecoveryRepair(sessionId, ref, io); }
      catch (error) {
        // Cancellation/deadline at the non-atomic B4/B5 boundary does not erase
        // historical evidence. Observed authority/storage failure does.
        if (!(error instanceof RecoveryAdmissionError) || !["RECOVERY_CONFLICT", "RECOVERY_ABORTED", "RECOVERY_DEADLINE"].includes(error.code)) engine.invalidateRepairResolution();
        throw error;
      }
      const after = readState();
      const now = authenticate(after, input);
      const { carriers: _priorCarriers, ...priorOriginal } = binding;
      const { carriers: _currentCarriers, ...currentOriginal } = now;
      if (!same(priorOriginal, currentOriginal)) return required("Named B5 original/delivery/pin binding changed during repair recording");
      if (!old && !canCreateForOpenOriginal(after, input.originalInputId)) return conflict("Original became terminal/noncurrent after B4 publication");
      checkJoined(ref, executionLeafId);
      return ref;
    },
  };
  return Object.freeze(provider);
}
