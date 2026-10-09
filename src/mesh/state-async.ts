import type { MeshStateEntry } from "./state-file.js";
import type { MeshIdentity } from "./event-log.js";
import type { FabricParticipantSource } from "../topology/types.js";
import type { StateBackendDeleteInput, StateBackendPutInput } from "./state-backend.js";
import { NatsKvStateStore, type NatsKvStateStoreOptions } from "./state-nats-kv.js";
import { MeshStore } from "./store.js";

/** Common SINGLE-KEY contract. Not a substitute for StateBackend's sync reads or writeBatch. */
export interface AsyncMeshStateStore {
  readonly kind: "file" | "sqlite" | "nats-kv";
  get(key: string): Promise<MeshStateEntry | undefined>;
  put(input: StateBackendPutInput): Promise<MeshStateEntry>;
  delete(input: StateBackendDeleteInput): Promise<{ deleted: boolean; version?: number }>;
  list(prefix?: string, limit?: number): Promise<MeshStateEntry[]>;
  listAll(prefix?: string): Promise<MeshStateEntry[]>;
  close(): Promise<void>;
}
export type AsyncMeshStateStoreOptions =
  | { backend?: "file" | "sqlite" }
  | { backend: "nats-kv"; nats: NatsKvStateStoreOptions };

/** Explicit experimental selector, independent of mesh.stateBackend (unchanged). Default is file. */
/** Opt-in mesh tools for independent shared/ keys. All other state/events stay on the supplied store. */
export const openNatsMeshProvider = async (
  store: MeshStore,
  identity: MeshIdentity,
  participants: FabricParticipantSource,
  options: Extract<AsyncMeshStateStoreOptions, { backend: "nats-kv" }>,
) => {
  if (options.backend !== "nats-kv" || options.nats?.experimentalNatsKv !== true) {
    throw new Error("Async mesh tools require backend: nats-kv and experimentalNatsKv: true");
  }
  const { MeshProvider } = await import("../providers/mesh-provider.js");
  return MeshProvider.withStateBackend(store, identity, participants, options);
};

export const openAsyncMeshStateStore = async (root: string, options: AsyncMeshStateStoreOptions = {}): Promise<AsyncMeshStateStore> => {
  if (options.backend === "nats-kv") {
    if (options.nats.experimentalNatsKv !== true) throw new Error("NATS KV state requires experimentalNatsKv: true");
    return NatsKvStateStore.open(root, options.nats);
  }
  const kind = options.backend ?? "file";
  const store = new MeshStore(root, 128 * 1024, 1_000, { stateBackend: kind });
  if (store.stateBackend !== kind) { store.closeState(); throw new Error(`Requested ${kind} state backend is unavailable`); }
  return {
    kind,
    get: async key => store.get(key),
    put: input => store.put(input),
    delete: input => store.delete(input),
    list: async (prefix, limit) => store.list(prefix, limit),
    listAll: async prefix => store.listAll(prefix),
    close: async () => { store.closeState(); },
  };
};
