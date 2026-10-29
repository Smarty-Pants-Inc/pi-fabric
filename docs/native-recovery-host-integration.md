# Native recovery host integration: blocked, not a default implementation

The native maintenance binding in this branch is not a product-ready default host.
`createFabricExtension()` registers it only when host code supplies `nativeRecovery`.
REQUIRED startup without that host remains fail-closed. An extension registration or
a passing fixture is not evidence that the missing authorities exist.

## Exact missing owner connections

| Required port | Available implementation / missing connection |
| --- | --- |
| B2 exclusion and predecessor custody | `PortableIdentityAdapter.validate/verifyPredecessor` are discovery contracts. `FabricRuntimeState` activates the transport lease after `session_start`; there is no native pre-open custody implementation in this tree. A receipt-shaped object or lease timeout cannot substitute for physical writer exclusion. |
| B5 input selection, outcomes and release driver | This tree's `RecoveryInputAdapter` has only `readRecoveryInput/admitContinuation`. The read-only B5 pin `6993174850bfd1460f2d3be2ca3f40376c63d1c5` exports the newer owner, select, permission, delivery and outcome functions, but has no extension factory registering them into this tree. B3 must not import an absolute sibling worktree or copy its ledger. |
| B4 repair and compaction validation | `prepareSessionRecovery` settles issued intents and materializes results, but does not provide an original-generation native `RepairRef`, protected-cut validation, native terminal verification or execution-branch restoration. Its callback is not a replacement `NativeRecoveryHost`. |
| B6 store and descriptor provenance | `RecoveryDurabilityStore.open` is real. It requires exact bound store/session/file and `verifyNativeEntry` provenance from the actual native owner. No default host currently supplies that provenance/lifetime. A directory name alone is not store authority. |
| Native current binding and maintenance scope | The SDK context supplies `assertMaintenanceScope` and the native manager owns exclusion/descriptors. These must be connected to the same current owner and epoch as B2/B5/B6, not a second in-memory epoch variable. |

`tests/native-maintenance-binding.test.ts` covers composition refusal and gate behavior.
The Pi lifecycle integration uses real native JSONL, B5 and B6, but its base host has
explicit test projection/validation ports; it is not this missing production host.

## Required completion

Co-version the registered native B5 owner/driver and B2/B4 native authority ports;
then compose `NativeRecoveryHost` at the default Main entry point before SDK binding.
Use the native current context for every epoch/scope check and preserve actual lease
activation before adoption. Only after that composition is exercised with real repair,
terminal and descriptor provenance can default REQUIRED startup be called integrated.
Do not remove REQUIRED checks, route through the legacy startup coordinator, invent
no-op validators, or use the test fixture as production code to make startup green.

This document records the bounded continuation's unresolved item 1. It does not
establish the whole-host, every-session, 120-second, zero-loss/duplicate qualification.
