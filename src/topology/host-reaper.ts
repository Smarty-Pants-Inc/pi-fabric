import { createHash } from "node:crypto";
import type { MeshBatchOperation, MeshBatchView, MeshIdentity, MeshStateEntry, MeshStore } from "../mesh/store.js";
import { type FabricHostLease, hostEntryLiveness, hostLeasePath, hostLeasePredecessor,
  readHostLeases, removeHostLeaseVersionUnderCustody, sameHostLeaseVersion } from "./host-leases.js";
import { isMeshLockTimeout } from "../core/atomic-write.js";
import { participantFilePresent, readParticipantFiles, removeParticipantFileIf, sweepParticipantLockLeftovers } from "./participant-files.js";
import { isLiveLegacyRootEntry } from "./legacy-root-liveness.js";
import { effectiveLiveness } from "./liveness.js";
import { meshDirectoryStamp } from "./publication-generation.js";

/** A host's records are removed when its lease expired this long ago (smarty-dev#367). */
export const DEAD_HOST_RECORDS_MS = 6 * 60 * 60 * 1000;
/** Bound dead host/participant cleanup; stale bookkeeping joins the same commit below. */
const MAX_REAP_BATCH = 500;

const HOST_PREFIX = "topology/hosts/";
const PARTICIPANT_PREFIX = "topology/participants/";
const INBOX_PREFIX = "topology/inbox/";
const SESSION_PREFIX = "sessions/";
/** Keep resumable inbox state and terminal session snapshots for at least a day. */
const DIRECTORY_RETENTION_MS = 24 * 60 * 60 * 1000;
const TERMINAL_SESSION_STATUSES = new Set(["completed", "failed", "stopped", "timed_out"]);
// The same key scheme as the participant directory.
const hostKey = (id: string): string => HOST_PREFIX + createHash("sha256").update(id).digest("hex");

const record = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : undefined;

// A host lease entry that is gone for the window: its lease (or, without one, its last write)
// is older than the cutoff, and so is the host's file lease (smarty-dev#816).
const leaseGone = (entry: MeshStateEntry, cutoff: number, leases: ReadonlyMap<string, FabricHostLease>): boolean => {
  return hostEntryLiveness(entry, leases).expiresAt <= cutoff;
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
  leases: ReadonlyMap<string, FabricHostLease> = fileLeases(mesh),
): DeadRecord[] => {
  const cutoff = (options.now ?? Date.now()) - (options.deadAfterMs ?? DEAD_HOST_RECORDS_MS);
  const fresh = { fresh: true };
  const hosts = new Map<string, boolean>();
  const dead: DeadRecord[] = [];
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

// Participant absence is not proof of departure: legacy roots can advertise only a session,
// and an active host/lease can bridge a gap in participant publication. Retain conservatively
// on positive host liveness; malformed participant presence already fails closed below.
const liveRootCursorKeys = (
  view: Pick<MeshBatchView, "listAll">, mesh: { root?: string }, now: number,
  leases: ReadonlyMap<string, FabricHostLease> = fileLeases(mesh),
): Set<string> => {
  const keys = new Set<string>();
  const keep = (id: unknown): void => {
    if (typeof id === "string") keys.add(INBOX_PREFIX + createHash("sha256").update(id).digest("hex").slice(0, 32));
  };
  for (const entry of view.listAll(SESSION_PREFIX)) {
    if (isLiveLegacyRootEntry(entry, now, mesh.root)) keep(entry.value.id);
  }
  for (const entry of view.listAll(HOST_PREFIX)) {
    const host = record(entry.value);
    if (typeof host?.id !== "string" || entry.key !== hostKey(host.id)) continue;
    if (hostEntryLiveness(entry, leases).expiresAt >= now) {
      keep(host.rootId);
      keep(record(host.identity)?.id);
    }
  }
  // A file-only host lease can outlive a shared-state publication gap too.
  for (const lease of leases.values()) {
    if (effectiveLiveness(undefined, lease).expiresAt >= now) {
      keep(lease.rootId);
      keep(lease.identityId);
    }
  }
  return keys;
};

// Inbox keys hash the writer's participant id to 32 hex chars (RootInbox.key). Raw state/file
// presence counts even if malformed or unreadable: absence, not failed parsing, permits cleanup.
const staleDirectoryDeletes = (
  mesh: Pick<MeshStore, "listAll"> & { root?: string }, identity: MeshIdentity, now: number,
): MeshBatchOperation[] => {
  const cutoff = now - DIRECTORY_RETENTION_MS;
  const participants = new Set(mesh.listAll(PARTICIPANT_PREFIX, { fresh: true }).map((entry) => entry.key));
  const live = liveRootCursorKeys({ listAll: (prefix) => mesh.listAll(prefix, { fresh: true }) }, mesh, now);
  const filePresent = (key: string): boolean => typeof mesh.root === "string" && participantFilePresent(mesh.root, key);
  const ops: MeshBatchOperation[] = [];
  for (const entry of mesh.listAll(INBOX_PREFIX, { fresh: true })) {
    const id = record(entry.updatedBy)?.id;
    if (typeof id !== "string" || !Number.isFinite(entry.updatedAt) || !(entry.updatedAt < cutoff)) continue;
    const hash = createHash("sha256").update(id).digest("hex");
    const participantKey = PARTICIPANT_PREFIX + hash;
    if (id === identity.id || entry.key !== INBOX_PREFIX + hash.slice(0, 32) ||
      participants.has(participantKey) || filePresent(participantKey) || live.has(entry.key)) continue;
    ops.push({
      kind: "delete", key: entry.key, ifVersion: entry.version, onConflict: "skip",
      // A resumed participant or a cursor updated after selection must survive this pass.
      condition: (current) => current(participantKey) === undefined && !filePresent(participantKey),
    });
  }
  for (const entry of mesh.listAll(SESSION_PREFIX, { fresh: true })) {
    const status = record(entry.value)?.status;
    // Idle/running/stopping sessions are NOT terminal, however old their last write is.
    if (Number.isFinite(entry.updatedAt) && entry.updatedAt < cutoff &&
      typeof status === "string" && TERMINAL_SESSION_STATUSES.has(status)) {
      ops.push({ kind: "delete", key: entry.key, ifVersion: entry.version, onConflict: "skip" });
    }
  }
  return ops;
};

/**
 * Deletes dead host records and stale directory bookkeeping in one batch. Every delete is
 * version-fenced; host/participant deletes additionally require their host still be gone
 * at commit: the two scans above are separate reads, and a host that renews after
 * them (with or without rewriting its participants) must keep every record (smarty-dev#367).
 * Returns how many it removed.
 */
export const reapDeadHostRecords = async (
  mesh: Pick<MeshStore, "listAll" | "writeBatch" | "custody"> & Partial<Pick<MeshStore, "leaseCustody">> & { root?: string },
  identity: MeshIdentity,
  options: { ownHostId: string; now?: number; deadAfterMs?: number;
    withCommitFence?: <T>(operation: () => T | Promise<T>) => Promise<T> },
): Promise<number> => {
  const fenced = options.withCommitFence ?? (async <T>(operation: () => T | Promise<T>): Promise<T> => operation());
  const cutoff = (options.now ?? Date.now()) - (options.deadAfterMs ?? DEAD_HOST_RECORDS_MS);
  const selectedLeases = fileLeases(mesh);
  const found = deadHostRecords(mesh, options, selectedLeases);
  const bookkeeping = staleDirectoryDeletes(mesh, identity, options.now ?? Date.now());
  // Leftovers of per-key lock operations whose process died (security pass S5 on #142).
  if (typeof mesh.root === "string") {
    const root = mesh.root;
    // Recovery can restore a detached key lock. A sweep must not unlink its owner
    // between the detach and the recovery recheck (smarty-dev#2570, P3 sweep).
    await sweepParticipantLockLeftovers({ root, custody: (operation) => mesh.custody(operation) }, 60 * 60 * 1000)
      .catch(() => undefined);
  }
  if (found.length === 0 && bookkeeping.length === 0) return 0;
  const dead = found.filter((item) => !item.file);
  // File leases are read before the state transaction and revalidated in it by their directory
  // stamp (smarty-dev#6477 R11): only a lease written in between is read again under the lock.
  const leaseStamp = typeof mesh.root === "string" ? meshDirectoryStamp(mesh.root, "host-leases") : "";
  const leasesBefore = fileLeases(mesh);
  let leases = leasesBefore;
  const targets = [...new Set(found.map(item => item.hostId))].sort();
  const changed = new Error("Reaper target lease renewed or replaced");
  const root = mesh.root;
  const assertTarget = (hostId: string, expected: FabricHostLease | undefined): void => {
    if (typeof root === "string" && !sameHostLeaseVersion(hostLeasePredecessor(root, hostId), expected)) throw changed;
  };
  const targetGate = <T>(hostId: string, operation: () => T | Promise<T>): Promise<T> =>
    typeof root === "string" && mesh.leaseCustody
      ? mesh.leaseCustody(hostLeasePath(root, hostId), operation)
      : Promise.resolve().then(operation);
  // Acquire target gates in a stable order, before the reaper's gate and shared state.
  // A renewal on ANY selected target aborts this pass before any record/file deletion.
  const targetGates = <T>(operation: () => T | Promise<T>, index = 0): Promise<T> => index === targets.length
    ? Promise.resolve().then(operation)
    : targetGate(targets[index]!, () => { assertTarget(targets[index]!, selectedLeases.get(targets[index]!)); return targetGates(operation, index + 1); });
  const retired = new Set<string>();
  const reapable = new Set<string>();
  let results;
  try {
    results = await targetGates(() => fenced(() => mesh.writeBatch({
      identity,
      ops: dead.map(({ entry, hostId }) => ({
        kind: "delete" as const, key: entry.key, ifVersion: entry.version, onConflict: "skip" as const,
        condition: (current: (key: string) => MeshStateEntry | undefined) => {
          const host = current(hostKey(hostId));
          return host === undefined || leaseGone(host, cutoff, leases);
        },
      })),
      prepare: view => {
        leases = typeof root !== "string" || meshDirectoryStamp(root, "host-leases") === leaseStamp
          ? leasesBefore : fileLeases(mesh);
        for (const hostId of targets) {
          const host = view.get(hostKey(hostId));
          if (!host || leaseGone(host, cutoff, leases)) reapable.add(hostId);
        }
        const live = liveRootCursorKeys(view, mesh, options.now ?? Date.now(), leases);
        return bookkeeping.filter(op => !live.has(op.key));
      },
      beforeCommit: () => { for (const hostId of targets) assertTarget(hostId, selectedLeases.get(hostId)); },
      // Still inside every target's gate. Renew/claim cannot cross the state COMMIT
      // and exact-version lease removal. Never re-read/adopt a later renewal as a victim.
      commitOutbox: () => {
        if (typeof root !== "string") return;
        for (const hostId of reapable) {
          const expected = selectedLeases.get(hostId);
          if (expected && effectiveLiveness(undefined, expected).expiresAt <= cutoff &&
            removeHostLeaseVersionUnderCustody(root, expected)) retired.add(hostId);
        }
      },
    })));
  } catch (error) {
    if (error === changed || isMeshLockTimeout(error)) return 0;
    throw error;
  }
  let removed = results.filter(result => result.applied).length;
  if (typeof root === "string") {
    // Lock order remains participant key -> target gate -> own gate -> state.
    // A new claim after the shared commit keeps ALL remaining participant files.
    for (const { entry, hostId } of found.filter(item => item.file)) {
      const gone = await removeParticipantFileIf({ root, custody: operation => mesh.custody(operation) }, entry.key, current => {
        const host = mesh.listAll(hostKey(hostId), { fresh: true }).find(candidate => candidate.key === hostKey(hostId));
        return current.version === entry.version && current.updatedAt === entry.updatedAt &&
          (!host || leaseGone(host, cutoff, fileLeases(mesh)));
      }, { withCommitFence: operation => targetGate(hostId, () => fenced(() => {
        assertTarget(hostId, retired.has(hostId) ? undefined : selectedLeases.get(hostId));
        return operation();
      })) }).catch(() => false);
      if (gone) removed += 1;
    }
  }
  return removed;
};
