// mesh.sync durability refusals (smarty-dev#9313, pi-fabric#894). Dependency-free so every backend
// (events, file state, NATS state) can share one error shape without widening the import graph.

/** Stable per-backend codes; existing codes never change. */
export type MeshSyncNotDurableCode =
  | "FABRIC_MESH_EVENTS_NOT_DURABLE"
  | "FABRIC_MESH_STATE_FILE_NOT_DURABLE"
  | "FABRIC_MESH_STATE_NATS_NOT_DURABLE";

/**
 * A mesh durability barrier (`MeshStore.sync`, host `mesh.sync`) that cannot confirm durability on this
 * backend or platform. Never retryable: the caller must not advance an external cursor or delivered mark
 * on it. `code` names the backend that refused.
 */
export class MeshSyncNotDurableError extends Error {
  readonly retryable = false;
  constructor(readonly code: MeshSyncNotDurableCode, subject: string, reason: string) {
    super(`${code}: Fabric mesh ${subject} durability is not confirmed: ${reason}; not durable`);
    this.name = "MeshSyncNotDurableError";
  }
}

/**
 * `EventLog.sync()` on a platform with no namespace barrier (Windows: syncPathNamespace and the archive
 * skip the directory fsync). events.jsonl is created and replaced by renames (compaction) and archive
 * segments are created in place, so an fsync of the file cannot confirm a committed event survives a
 * power loss.
 */
export class MeshEventsNotDurableError extends MeshSyncNotDurableError {
  constructor(readonly platform: NodeJS.Platform) {
    super("FABRIC_MESH_EVENTS_NOT_DURABLE", "event", `events.jsonl is replaced by a rename on compaction, archive segments are new files, and ${platform} has no directory fsync barrier for them, so a committed event may not survive a power loss`);
    this.name = "MeshEventsNotDurableError";
  }
}

export const meshEventsNotDurable = (platform: NodeJS.Platform): MeshEventsNotDurableError => new MeshEventsNotDurableError(platform);
