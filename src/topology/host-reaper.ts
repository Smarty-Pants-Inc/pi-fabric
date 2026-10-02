import { createHash } from "node:crypto";
import type { MeshIdentity, MeshStateEntry, MeshStore } from "../mesh/store.js";
import { type FabricHostLease, readHostLeases, removeHostLease } from "./host-leases.js";
import { readParticipantFiles, removeParticipantFileIf, sweepParticipantLockLeftovers } from "./participant-files.js";

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
// is older than the cutoff, and so is the host's file lease (smarty-dev#816).
const leaseGone = (entry: MeshStateEntry, cutoff: number, leases: ReadonlyMap<string, FabricHostLease>): boolean => {
  const value = record(entry.value);
  const expiresAt = value?.expiresAt;
  const file = typeof value?.id === "string" ? leases.get(value.id)?.expiresAt : undefined;
  return (typeof expiresAt === "number" ? expiresAt : entry.updatedAt) <= cutoff && (file === undefined || file <= cutoff);
};

// Hosts' file leases, when the store has a root to read them from.
const fileLeases = (mesh: { root?: string }): ReadonlyMap<string, FabricHostLease> =>
  typeof mesh.root === "string" ? readHostLeases(mesh.root) : new Map();

/** `file`: the record's own file (smarty-dev#2004), not a shared-state entry. */
interface DeadRecord { entry: MeshStateEntry; hostId: string; file?: true }

/**
 * Records left in the shared state by hosts that are gone: host leases that expired longer ago
 * than the window, and participants whose owner host is such a host, or has no record and the
 * participant was not written within the window. A host deletes its own records only on a clean
 * shutdown, so killed or crashed hosts left them for good, and every runtime parsed them.
 */
export const deadHostRecords = (
  mesh: Pick<MeshStore, "listAll"> & { root?: string },
  options: { ownHostId: string; now?: number; deadAfterMs?: number },
): DeadRecord[] => {
  const cutoff = (options.now ?? Date.now()) - (options.deadAfterMs ?? DEAD_HOST_RECORDS_MS);
  const fresh = { fresh: true };
  const hosts = new Map<string, boolean>();
  const dead: DeadRecord[] = [];
  const leases = fileLeases(mesh);
  for (const entry of mesh.listAll(HOST_PREFIX, fresh)) {
    const id = record(entry.value)?.id;
    if (typeof id !== "string" || entry.key !== hostKey(id)) continue;
    const gone = id !== options.ownHostId && leaseGone(entry, cutoff, leases);
    hosts.set(id, gone);
    if (gone) dead.push({ entry, hostId: id });
  }
  const files = typeof mesh.root === "string" ? readParticipantFiles(mesh.root) : [];
  for (const [entries, file] of [[mesh.listAll(PARTICIPANT_PREFIX, fresh), false], [files, true]] as const) {
    for (const entry of entries) {
      const owner = record(entry.value)?.ownerHostId;
      if (typeof owner !== "string" || owner === options.ownHostId) continue;
      const gone = hosts.get(owner);
      if (gone === true || (gone === undefined && entry.updatedAt <= cutoff)) {
        dead.push({ entry, hostId: owner, ...(file ? { file: true as const } : {}) });
      }
    }
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
  mesh: Pick<MeshStore, "listAll" | "writeBatch" | "exclusive"> & { root?: string },
  identity: MeshIdentity,
  options: { ownHostId: string; now?: number; deadAfterMs?: number },
): Promise<number> => {
  const cutoff = (options.now ?? Date.now()) - (options.deadAfterMs ?? DEAD_HOST_RECORDS_MS);
  const found = deadHostRecords(mesh, options);
  // Leftovers of per-key lock operations whose process died (security pass S5 on #142).
  if (typeof mesh.root === "string") {
    const root = mesh.root;
    // Recovery can restore a detached key lock. A sweep must not unlink its owner
    // between the detach and the recovery recheck (smarty-dev#2570, P3 sweep).
    await sweepParticipantLockLeftovers({ root, exclusive: (operation) => mesh.exclusive(operation) }, 60 * 60 * 1000)
      .catch(() => undefined);
  }
  if (found.length === 0) return 0;
  const dead = found.filter((item) => !item.file);
  const results = dead.length === 0 ? [] : await mesh.writeBatch({
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
        return host === undefined || leaseGone(host, cutoff, fileLeases(mesh));
      },
    })),
  });
  let removed = results.filter((result) => result.applied).length;
  if (typeof mesh.root === "string") {
    // Participant files, each checked again just before its removal: its host must still be gone
    // (or have no record, where the orphan rule held at selection), and the file unchanged since.
    // Under the file's own lock, so a takeover since the scan keeps its file (review/astra F1 on #142).
    // ponytail: a host renewing only its lease file is not fenced; it has been gone for hours.
    for (const { entry, hostId } of found.filter((item) => item.file)) {
      const hostGone = (): boolean => {
        const host = mesh.listAll(hostKey(hostId), { fresh: true }).find((candidate) => candidate.key === hostKey(hostId));
        return !host || leaseGone(host, cutoff, fileLeases(mesh));
      };
      const gone = await removeParticipantFileIf({ root: mesh.root, exclusive: (operation) => mesh.exclusive(operation) }, entry.key, (current) =>
        current.version === entry.version && current.updatedAt === entry.updatedAt && hostGone()).catch(() => false);
      if (gone) removed += 1;
    }
    // A reaped host's file lease goes too, once it is as old.
    const leases = readHostLeases(mesh.root);
    for (const hostId of new Set(found.map((item) => item.hostId))) {
      const lease = leases.get(hostId);
      if (lease && lease.expiresAt <= cutoff) removeHostLease(mesh.root, hostId);
    }
  }
  return removed;
};
