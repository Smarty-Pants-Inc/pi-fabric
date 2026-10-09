import type { MeshStateEntry } from "./state-file.js";
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
