import type { MeshIdentity, MeshStateEntry, MeshStore } from "../mesh/store.js";

/** A host's records are removed when its lease expired this long ago (smarty-dev#367). */
export const DEAD_HOST_RECORDS_MS = 6 * 60 * 60 * 1000;
/** At most this many deletes in one sweep, so one write stays small. */
const MAX_REAP_BATCH = 500;

const HOST_PREFIX = "topology/hosts/";
const PARTICIPANT_PREFIX = "topology/participants/";

const record = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : undefined;

/**
 * Records left in the shared state by hosts that are gone: host leases that expired longer ago
 * than the window, and participants whose owner host is such a host, or has no record and the
 * participant was not written within the window. A host deletes its own records only on a clean
 * shutdown, so killed or crashed hosts left them for good, and every runtime parsed them.
 */
export const deadHostRecords = (
  mesh: Pick<MeshStore, "listAll">,
  options: { ownHostId: string; now?: number; deadAfterMs?: number },
): MeshStateEntry[] => {
  const cutoff = (options.now ?? Date.now()) - (options.deadAfterMs ?? DEAD_HOST_RECORDS_MS);
  const fresh = { fresh: true };
  const hosts = new Map<string, boolean>();
  const dead: MeshStateEntry[] = [];
  for (const entry of mesh.listAll(HOST_PREFIX, fresh)) {
    const host = record(entry.value);
    const id = typeof host?.id === "string" ? host.id : undefined;
    if (!id) continue;
    const expiresAt = typeof host?.expiresAt === "number" ? host.expiresAt : entry.updatedAt;
    const gone = id !== options.ownHostId && expiresAt <= cutoff;
    hosts.set(id, gone);
    if (gone) dead.push(entry);
  }
  for (const entry of mesh.listAll(PARTICIPANT_PREFIX, fresh)) {
    const owner = record(entry.value)?.ownerHostId;
    if (typeof owner !== "string" || owner === options.ownHostId) continue;
    const gone = hosts.get(owner);
    if (gone === true || (gone === undefined && entry.updatedAt <= cutoff)) dead.push(entry);
  }
  return dead.slice(0, MAX_REAP_BATCH);
};

/** Deletes them in one batch, each fenced to the version it saw; returns how many it removed. */
export const reapDeadHostRecords = async (
  mesh: Pick<MeshStore, "listAll" | "writeBatch">,
  identity: MeshIdentity,
  options: { ownHostId: string; now?: number; deadAfterMs?: number },
): Promise<number> => {
  const dead = deadHostRecords(mesh, options);
  if (dead.length === 0) return 0;
  const results = await mesh.writeBatch({
    identity,
    ops: dead.map((entry) => ({ kind: "delete" as const, key: entry.key, ifVersion: entry.version, onConflict: "skip" as const })),
  });
  return results.filter((result) => result.applied).length;
};
