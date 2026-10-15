// Lightweight public mesh entry (pi-fabric/mesh) for host scripts that run outside Pi,
// such as fleet schedulers and maintenance tools. It loads only the mesh store: no
// extension, UI, or agent runtime.
export { MeshBatchConflictError, MeshDedupeRecoveryError, MeshDedupeStoreFullError, MeshIncarnationPendingError, MeshLockTimeoutError,
  MeshSnapshotRetriesExhaustedError, MeshStore, MESH_INCARNATION_PENDING_CODE, MESH_SNAPSHOT_RETRIES_EXHAUSTED_CODE,
  isMeshIncarnationPending, isMeshSnapshotRetriesExhausted } from "./mesh/store.js";
export { MeshCustodyUnrecoverableError } from "./mesh/custody-lock.js";
export { MeshLockPreEntryRefusedError, MeshLockReleaseError, meshLockSecondaryReleaseErrors } from "./mesh/lock-release.js";
export { MeshLegacyDebrisUnrecoverableError } from "./mesh/legacy-debris.js";
export type { MeshLockOperationState, MeshLockReleaseFailureOutcome } from "./mesh/lock-release.js";
export type { FabricPrincipal, FabricPrincipalAuthorityCheck } from "./fabric-provenance.js";
export type {
  MeshBatchOperation,
  MeshBatchResult,
  MeshBatchView,
  MeshEvent,
  MeshIdentity,
  MeshReadOptions,
  MeshStateEntry,
  MeshStoreOptions,
  MeshTailResult,
} from "./mesh/store.js";
