import type { MeshStateEntry } from "../mesh/store.js";
import { readHostLease } from "./host-leases.js";
import { effectiveLiveness, type Liveness } from "./liveness.js";

/** The directory and reaper must agree about session-only root liveness. */
export const LEGACY_ROOT_LEASE_MS = 15_000;

type LegacyRootEntry = MeshStateEntry & {
  value: Record<string, unknown> & {
    id: string;
    sessionId: string;
    cwd: string;
    startedAt: number;
    status: "idle" | "running";
  };
};

/** Supported legacy advertisement, including key and writer attribution. */
export const isLiveLegacyRootEntry = (entry: MeshStateEntry, now: number, meshRoot?: string): entry is LegacyRootEntry => {
  if (!Number.isFinite(entry.updatedAt) || sessionLiveness(entry, meshRoot).expiresAt < now ||
    typeof entry.value !== "object" || entry.value === null || Array.isArray(entry.value)) return false;
  const value = entry.value as Record<string, unknown>;
  return typeof value.id === "string" &&
    typeof value.sessionId === "string" && entry.key === `sessions/${value.sessionId}` &&
    entry.updatedBy?.id === value.id && typeof value.cwd === "string" &&
    typeof value.startedAt === "number" && (value.status === "idle" || value.status === "running");
};

/** Session renewal shares its Main's small host file, but retains the fixed legacy TTL. */
export const sessionLiveness = (entry: MeshStateEntry, meshRoot?: string): Liveness => {
  const stored = { updatedAt: entry.updatedAt, expiresAt: entry.updatedAt + LEGACY_ROOT_LEASE_MS };
  if (!meshRoot || typeof entry.value !== "object" || entry.value === null) return stored;
  const value = entry.value as Record<string, unknown>;
  if (value.livenessLeaseFiles !== 1 || typeof value.id !== "string" ||
    typeof value.sessionId !== "string" || entry.key !== `sessions/${value.sessionId}` ||
    entry.updatedBy?.id !== value.id) return stored;
  const lease = readHostLease(meshRoot, typeof value.livenessHostId === "string" ? value.livenessHostId : value.id);
  const session = lease?.session;
  const matching = lease?.rootId === value.id && lease.identityId === entry.updatedBy?.id &&
    lease.startedAt === value.livenessStartedAt &&
    session?.id === value.sessionId && session.startedAt === value.startedAt;
  return effectiveLiveness(stored, matching ? session : undefined);
};
