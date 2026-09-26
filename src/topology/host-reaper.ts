import { createHash } from "node:crypto";
import type { MeshIdentity, MeshStateEntry, MeshStore } from "../mesh/store.js";

/** A host's records are removed when its lease expired this long ago (smarty-dev#367). */
export const DEAD_HOST_RECORDS_MS = 6 * 60 * 60 * 1000;
/** At most this many deletes in one sweep, so one write stays small. */
const MAX_REAP_BATCH = 500;

const HOST_PREFIX = "topology/hosts/";
const PARTICIPANT_PREFIX = "topology/participants/";
// The same key scheme as the participant directory.
const hostKey = (id: string): string => HOST_PREFIX + createHash("sha256").update(id).digest("hex");

const record = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : undefined;

// A host lease entry that is gone for the window: its lease (or, without one, its last write)
// is older than the cutoff.
const leaseGone = (entry: MeshStateEntry, cutoff: number): boolean => {
  const expiresAt = record(entry.value)?.expiresAt;
  return (typeof expiresAt === "number" ? expiresAt : entry.updatedAt) <= cutoff;
};

interface DeadRecord { entry: MeshStateEntry; hostId: string }

/**
 * Records left in the shared state by hosts that are gone: host leases that expired longer ago
 * than the window, and participants whose owner host is such a host, or has no record and the
 * participant was not written within the window. A host deletes its own records only on a clean
 * shutdown, so killed or crashed hosts left them for good, and every runtime parsed them.
 */
export const deadHostRecords = (
  mesh: Pick<MeshStore, "listAll">,
  options: { ownHostId: string; now?: number; deadAfterMs?: number },
): DeadRecord[] => {
  const cutoff = (options.now ?? Date.now()) - (options.deadAfterMs ?? DEAD_HOST_RECORDS_MS);
  const fresh = { fresh: true };
  const hosts = new Map<string, boolean>();
  const dead: DeadRecord[] = [];
  for (const entry of mesh.listAll(HOST_PREFIX, fresh)) {
    const id = record(entry.value)?.id;
    if (typeof id !== "string" || entry.key !== hostKey(id)) continue;
    const gone = id !== options.ownHostId && leaseGone(entry, cutoff);
    hosts.set(id, gone);
    if (gone) dead.push({ entry, hostId: id });
  }
  for (const entry of mesh.listAll(PARTICIPANT_PREFIX, fresh)) {
    const owner = record(entry.value)?.ownerHostId;
    if (typeof owner !== "string" || owner === options.ownHostId) continue;
    const gone = hosts.get(owner);
    if (gone === true || (gone === undefined && entry.updatedAt <= cutoff)) dead.push({ entry, hostId: owner });
  }
  return dead.slice(0, MAX_REAP_BATCH);
};

/**
 * Deletes them in one batch. Each delete is fenced to the version it saw and, at commit, to its
 * host still being gone: the two scans above are separate reads, and a host that renews after
 * them (with or without rewriting its participants) must keep every record (smarty-dev#367).
 * Returns how many it removed.
 */
export const reapDeadHostRecords = async (
  mesh: Pick<MeshStore, "listAll" | "writeBatch">,
  identity: MeshIdentity,
  options: { ownHostId: string; now?: number; deadAfterMs?: number },
): Promise<number> => {
  const cutoff = (options.now ?? Date.now()) - (options.deadAfterMs ?? DEAD_HOST_RECORDS_MS);
  const dead = deadHostRecords(mesh, options);
  if (dead.length === 0) return 0;
  const results = await mesh.writeBatch({
    identity,
    ops: dead.map(({ entry, hostId }) => ({
      kind: "delete" as const,
      key: entry.key,
      ifVersion: entry.version,
      onConflict: "skip" as const,
      // Absent at commit is gone: the host was deleted (earlier in this batch, or it had no
      // record, where the orphan rule held at selection and the version fence holds since).
      condition: (current: (key: string) => MeshStateEntry | undefined) => {
        const host = current(hostKey(hostId));
        return host === undefined || leaseGone(host, cutoff);
      },
    })),
  });
  return results.filter((result) => result.applied).length;
};
