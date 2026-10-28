import type { MeshIdentity, MeshStateEntry, MeshStore } from "../mesh/store.js";
import { isMeshWriteDeferred } from "../core/atomic-write.js";
import { type FabricHostLease, hostEntryLiveness, hostLeasePath, hostLeasePredecessor,
  readHostLeases, sameHostLeaseVersion } from "../topology/host-leases.js";
import { meshDirectoryStamp } from "../topology/publication-generation.js";
import { sessionLiveness } from "../topology/legacy-root-liveness.js";
import { effectiveLiveness } from "../topology/liveness.js";

/** A session counts as gone only when nothing has shown it alive for this long. */
export const DEAD_SESSION_PRESENCE_MS = 24 * 60 * 60 * 1000;

const PRESENCE_PREFIX = "actors/";
const HOST_PREFIX = "topology/hosts/";
const LEGACY_SESSION_PREFIX = "sessions/";

const record = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : undefined;

// The Fabric session a host lease belongs to: a Main (session:<id>) or a resident host of
// that root (rootId session:<id>).
const leaseSessions = (value: unknown): string[] => {
  const host = record(value);
  if (!host) return [];
  const identity = record(host.identity);
  return [host.id, host.rootId, identity?.id, identity?.sessionId]
    .filter((id): id is string => typeof id === "string")
    .map((id) => id.startsWith("session:") ? id.slice("session:".length) : id);
};

/**
 * Actor presence (actors/<session>/<actor>) left by sessions that are gone: no host lease or
 * legacy session entry of that session is newer than the window, and none of its presence
 * entries was written within it (smarty-dev#448). A live session renews its lease every
 * few seconds, and a session that comes back republishes its presence when it loads.
 */
export const deadSessionPresence = (
  mesh: Pick<MeshStore, "listAll"> & { root?: string },
  options: { ownSessionId: string; now?: number; deadAfterMs?: number },
): MeshStateEntry[] => {
  const cutoff = (options.now ?? Date.now()) - (options.deadAfterMs ?? DEAD_SESSION_PRESENCE_MS);
  const fresh = { fresh: true };
  const alive = new Set<string>([options.ownSessionId]);
  const leases = typeof mesh.root === "string" ? readHostLeases(mesh.root) : new Map();
  for (const entry of mesh.listAll(HOST_PREFIX, fresh)) {
    if (hostEntryLiveness(entry, leases).expiresAt <= cutoff) continue;
    for (const session of leaseSessions(entry.value)) alive.add(session);
  }
  for (const entry of mesh.listAll(LEGACY_SESSION_PREFIX, fresh)) {
    if (sessionLiveness(entry, mesh.root).updatedAt > cutoff) alive.add(entry.key.slice(LEGACY_SESSION_PREFIX.length));
  }
  // Hosts renew file leases outside the shared state (smarty-dev#816).
  if (typeof mesh.root === "string") {
    for (const lease of leases.values()) {
      if (effectiveLiveness(undefined, lease).expiresAt <= cutoff) continue;
      for (const session of leaseSessions({ id: lease.id, rootId: lease.rootId, identity: { id: lease.identityId } })) {
        alive.add(session);
      }
    }
  }
  const bySession = new Map<string, MeshStateEntry[]>();
  for (const entry of mesh.listAll(PRESENCE_PREFIX, fresh)) {
    const [session, actorId, ...rest] = entry.key.slice(PRESENCE_PREFIX.length).split("/");
    if (!session || !actorId || rest.length > 0 || record(entry.value)?.id !== actorId) continue;
    bySession.set(session, [...bySession.get(session) ?? [], entry]);
  }
  return [...bySession].flatMap(([session, entries]) =>
    alive.has(session) || entries.some((entry) => entry.updatedAt > cutoff) ? [] : entries);
};

/** Row CAS plus commit-time owner liveness; yield all custody between bounded batches. */
export const reapDeadSessionPresence = async (
  mesh: Pick<MeshStore, "listAll" | "writeBatch"> & Partial<Pick<MeshStore, "leaseCustody">> & { root?: string },
  identity: MeshIdentity,
  options: { ownSessionId: string; now?: number; deadAfterMs?: number },
): Promise<number> => {
  const dead = deadSessionPresence(mesh, options);
  if (dead.length === 0) return 0;
  const cutoff = (options.now ?? Date.now()) - (options.deadAfterMs ?? DEAD_SESSION_PRESENCE_MS);
  const root = mesh.root;
  // A file-backed adapter without target custody cannot fence file-only renewals.
  if (typeof root === "string" && !mesh.leaseCustody) return 0;
  let removed = 0;
  const changed = new Error("Presence reaper owner lease changed");
  const stamp = (): string => {
    try { return typeof root === "string" ? meshDirectoryStamp(root, "host-leases") : ""; }
    catch { throw changed; } // Unreadable is not evidence of death.
  };
  for (let start = 0; start < dead.length; start += 64) {
    const batch = dead.slice(start, start + 64);
    const sessionOf = (entry: MeshStateEntry): string => entry.key.slice(PRESENCE_PREFIX.length).split("/")[0]!;
    const sessions = new Set(batch.map(sessionOf));
    try {
      // Discover owners and parse files OUTSIDE state custody. The directory fence also
      // notices a new/replacement resident, not just renewals of captured host ids.
      const generation = stamp();
      const problems: string[] = [];
      const leases: ReadonlyMap<string, FabricHostLease> = typeof root === "string" ? readHostLeases(root, problems) : new Map();
      if (problems.length > 0 || stamp() !== generation) return removed;
      const hosts = mesh.listAll(HOST_PREFIX, { fresh: true });
      const legacy = new Map(mesh.listAll(LEGACY_SESSION_PREFIX, { fresh: true }).map(entry => [entry.key, entry]));
      const targets = new Set([...sessions].map(session => `session:${session}`));
      const alive = new Set<string>();
      const relevant = (value: unknown): boolean => leaseSessions(value).some(session => sessions.has(session));
      for (const host of hosts) {
        const id = record(host.value)?.id;
        if (typeof id === "string" && relevant(host.value)) targets.add(id);
      }
      for (const lease of leases.values()) {
        const value = { id: lease.id, rootId: lease.rootId, identity: { id: lease.identityId } };
        if (relevant(value)) targets.add(lease.id);
        if (effectiveLiveness(undefined, lease).expiresAt > cutoff || (lease.session?.updatedAt ?? 0) > cutoff) {
          for (const session of leaseSessions(value)) alive.add(session);
        }
      }
      for (const session of sessions) {
        const value = record(legacy.get(LEGACY_SESSION_PREFIX + session)?.value);
        const id = value?.livenessHostId ?? value?.id;
        if (typeof id === "string") targets.add(id);
      }
      const orderedTargets = [...targets].sort();
      const assertTarget = (id: string): void => {
        if (typeof root !== "string") return;
        try {
          if (sameHostLeaseVersion(hostLeasePredecessor(root, id), leases.get(id))) return;
        } catch { /* Invalid/unreadable bytes also veto the commit. */ }
        throw changed;
      };
      // Same order as the host reaper: target lease gates BEFORE shared state, held
      // through COMMIT. Compare exact renewal revisions, never adopt a later lease.
      const gated = <T>(operation: () => T | Promise<T>, index = 0): Promise<T> =>
        typeof root !== "string" || index === orderedTargets.length ? Promise.resolve().then(operation) :
          mesh.leaseCustody!(hostLeasePath(root, orderedTargets[index]!), () => {
            assertTarget(orderedTargets[index]!);
            return gated(operation, index + 1);
          });
      const results = await gated(() => mesh.writeBatch({
        identity,
        lockTiming: { operation: "presence-reap" },
        ops: batch.map(entry => {
          const session = sessionOf(entry);
          const key = LEGACY_SESSION_PREFIX + session;
          return {
            kind: "delete" as const, key: entry.key, ifVersion: entry.version, onConflict: "skip" as const,
            condition: (current: (key: string) => MeshStateEntry | undefined) => {
              const owner = current(key);
              return !alive.has(session) && owner?.version === legacy.get(key)?.version &&
                (owner === undefined || owner.updatedAt <= cutoff);
            },
          };
        }),
        prepare: view => {
          // Only the small host directory, never I/O or an actor scan/planner under state custody.
          for (const host of view.listAll(HOST_PREFIX)) {
            if (!relevant(host.value)) continue;
            const id = record(host.value)?.id;
            if (typeof id !== "string" || !targets.has(id) || !(hostEntryLiveness(host, leases).expiresAt <= cutoff)) {
              for (const session of leaseSessions(host.value)) alive.add(session);
            }
          }
          return [];
        },
        beforeCommit: () => {
          for (const id of orderedTargets) assertTarget(id);
          if (stamp() !== generation) throw changed;
        },
      }));
      removed += results.filter(result => result.applied).length;
    } catch (error) {
      if (error === changed || isMeshWriteDeferred(error)) return removed;
      throw error;
    }
  }
  return removed;
};
