export { MeshListingIncompleteError } from "./mesh/listing.js";
export { openAsyncMeshStateStore, openNatsMeshProvider } from "./mesh/state-async.js";
export type { AsyncMeshStateStore, AsyncMeshStateStoreOptions } from "./mesh/state-async.js";
export { NatsKvStateStore, NatsKvListTimeoutError, encodeNatsKvKey, decodeNatsKvKey, natsKvPrefixFilter, natsKvBucketForRoot,
  isSupportedNatsKvServer, NATS_KV_MIN_SERVER_VERSION, NATS_KV_MAX_VALUE_BYTES, NATS_KV_MAX_KEYS } from "./mesh/state-nats-kv.js";
export type { NatsKvStateStoreOptions, NatsKvListPage, NatsKvStateChange, NatsKvStateWatch, NatsKvStateWatchOptions } from "./mesh/state-nats-kv.js";

// Lightweight public mesh entry (pi-fabric/mesh) for host scripts that run outside Pi,
// such as fleet schedulers and maintenance tools. It loads only the mesh store: no
// extension, UI, or agent runtime.
export { MeshBatchConflictError, MeshDedupeRecoveryError, MeshDedupeStoreFullError, MeshLockTimeoutError, MeshStore } from "./mesh/store.js";
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
