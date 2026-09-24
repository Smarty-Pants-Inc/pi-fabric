// Lightweight public mesh entry (pi-fabric/mesh) for host scripts that run outside Pi,
// such as fleet schedulers and maintenance tools. It loads only the mesh store: no
// extension, UI, or agent runtime.
export { MeshBatchConflictError, MeshStore } from "./mesh/store.js";
export type {
  MeshBatchOperation,
  MeshBatchResult,
  MeshEvent,
  MeshIdentity,
  MeshStateEntry,
  MeshStoreOptions,
  MeshTailResult,
} from "./mesh/store.js";
