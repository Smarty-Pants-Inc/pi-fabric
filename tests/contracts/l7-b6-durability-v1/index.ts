/** Stable source-tree entrypoint for the TEST-ONLY B6 durability contract. */
export type {
  RecoveryDurabilityAdapter, RecoveryDurableAppendRequest, RecoveryDurableAppendResult,
  RecoveryDurableEntryRequest, RecoveryDurableEntryResult,
  RecoveryNativeEntryLocation, RecoveryNativeFlushReceipt,
  RecoveryNativeEntryRegistrationRequest, RecoveryNativeEntryRegistrationResult,
  B6CommitGate, B6CommittedEntryObservation, B6DurabilityHarness, B6DurabilityHarnessFactory,
  B6HarnessObservation, B6NativeDescriptorFixtureOptions, B6RecoveryState,
  B6ReceiptKind, B6RegistrationObservation,
} from "./contract.js";
export { runConformance } from "./conformance.js";
export { createTestOnlyFakeHarness, createTestOnlyFakeHarnessFactory } from "./fake.js";
