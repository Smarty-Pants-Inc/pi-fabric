/**
 * The mesh state backend kinds (smarty-dev#7504). A leaf with no imports: config.ts parses
 * `mesh.stateBackend` with it without pulling SQLite into the config graph, and state-backend.ts
 * keys its factory registry on it. See docs/mesh-backends.md.
 */

/** Every kind the selector knows. `nats` is a slot: its factory refuses until fabric-v2's store lands. */
export const MESH_STATE_BACKEND_KINDS = ["file", "shadow", "sqlite", "nats"] as const;

/** The configured state backend (`mesh.stateBackend`, env `PI_FABRIC_MESH_STATE_BACKEND`). */
export type MeshStateBackendKind = typeof MESH_STATE_BACKEND_KINDS[number];

export const isMeshStateBackendKind = (value: unknown): value is MeshStateBackendKind =>
  typeof value === "string" && (MESH_STATE_BACKEND_KINDS as readonly string[]).includes(value);

export const MESH_STATE_BACKEND_KIND_LIST = "file, shadow, sqlite or nats";
