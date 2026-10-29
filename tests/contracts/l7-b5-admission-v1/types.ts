import type {
  initializeNewRecoverySession, recordRecoveryRepair, NewRecoverySessionInitialization,
  RecoveryGenerationPin, RecoveryInputAdapter,
} from "../../../src/recovery/input-admission.js";
import type { RecoveryMaintenanceAdapter, RepairRef } from "../../../src/recovery/maintenance-admission.js";

/** Test-only C1/C2 consumer surface; deliberately NOT a full maintenance/native adapter. */
export type B5Admission = Pick<RecoveryInputAdapter,
  "readRecoveryInput" | "ensureRecoveryGeneration" | "admitContinuation" | "recordInputOutcome"
> & {
  initializeNewRecoverySession: typeof initializeNewRecoverySession;
  recordRecoveryRepair: typeof recordRecoveryRepair;
  maintenance: Pick<RecoveryMaintenanceAdapter, "read" | "listPending">;
};

/** Harness evidence, not a production permission or a serialized ledger ABI. */
export interface AdmissionObservation {
  readonly revision: number | undefined;
  readonly pins: readonly RecoveryGenerationPin[];
  readonly continuationIds: readonly string[];
  readonly repairs: readonly RepairRef[];
  /** Opaque exact storage identity/content fingerprint, including absent namespaces. */
  readonly storage: string;
  readonly nativeDeliveries: number;
}
export interface AdmissionHarness {
  readonly admission: B5Admission;
  readonly sessionId: string;
  readonly originalInputId: string;
  readonly deadlineMs: number;
  readonly initialization: NewRecoverySessionInitialization;
  /** Positive fixture identity changes monotonically NEW -> REQUIRED. */
  requireIdentity(): void;
  bindRequired(): void;
  unbind(): void;
  /** Same root/session, new full process/binding epoch; NEVER initializes. */
  rebindRequired(): void;
  /** Admit/select/deliver/interrupt/actually join at a fixture boundary; no pin yet. */
  seedJoinedOriginal(): Promise<void>;
  repairFor(recoveryGeneration: string): RepairRef;
  observe(): AdmissionObservation;
  removeLedger(): void;
  /** One visible commit followed by an injected acknowledgement failure. */
  withLostWriteAck<T>(write: () => T | Promise<T>): Promise<T>;
  /** Idempotent; unbinds, restores owned seams and removes ONLY this harness's temp. */
  close(): void;
}
export interface AdmissionHarnessFactory {
  readonly name: string;
  create(): AdmissionHarness | Promise<AdmissionHarness>;
}
export const UNKNOWN_ACK_MESSAGE = "test-only B5 write acknowledgement lost after publish";
