/** Stable source-tree entrypoint for TEST-ONLY C1/C2 consumers (Vitest required). */
export type { B5Admission, AdmissionHarness, AdmissionHarnessFactory, AdmissionObservation } from "./types.js";
export { runConformance } from "./conformance.js";
export { createTestOnlyFakeHarness } from "./fake.js";
export type {
  NewRecoverySessionInitialization, RecoveryAdmissionOwner, RecoveryIo, RecoveryGenerationPin,
  RecoveryInputAdapter, RecoveryInputSnapshot, RecoveryInputOutcomeRequest, RecoveryInputOutcomeResult,
} from "../../../src/recovery/input-admission.js";
export type {
  RecoveryEpoch, RepairRef, MaintenanceFailureCode, MaintenanceRefusal, MaintenanceResult,
  MaintenanceKey, MaintenanceSnapshot, PendingMaintenance,
} from "../../../src/recovery/maintenance-admission.js";
