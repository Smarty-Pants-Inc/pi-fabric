import type { MeshIdentity, MeshStateEntry, MeshStore } from "../mesh/store.js";

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
  mesh: Pick<MeshStore, "listAll">,
  options: { ownSessionId: string; now?: number; deadAfterMs?: number },
): MeshStateEntry[] => {
  const cutoff = (options.now ?? Date.now()) - (options.deadAfterMs ?? DEAD_SESSION_PRESENCE_MS);
  const fresh = { fresh: true };
  const alive = new Set<string>([options.ownSessionId]);
  for (const entry of mesh.listAll(HOST_PREFIX, fresh)) {
    const expiresAt = record(entry.value)?.expiresAt;
    if ((typeof expiresAt === "number" ? expiresAt : entry.updatedAt) <= cutoff) continue;
    for (const session of leaseSessions(entry.value)) alive.add(session);
  }
  for (const entry of mesh.listAll(LEGACY_SESSION_PREFIX, fresh)) {
    if (entry.updatedAt > cutoff) alive.add(entry.key.slice(LEGACY_SESSION_PREFIX.length));
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

/** Deletes dead sessions' presence in one batch, each fenced to the version it was seen at. */
export const reapDeadSessionPresence = async (
  mesh: Pick<MeshStore, "listAll" | "writeBatch">,
  identity: MeshIdentity,
  options: { ownSessionId: string; now?: number; deadAfterMs?: number },
): Promise<number> => {
  const dead = deadSessionPresence(mesh, options);
  if (dead.length === 0) return 0;
  const results = await mesh.writeBatch({
    identity,
    ops: dead.map((entry) => ({ kind: "delete" as const, key: entry.key, ifVersion: entry.version, onConflict: "skip" as const })),
  });
  return results.filter((result) => result.applied).length;
};
