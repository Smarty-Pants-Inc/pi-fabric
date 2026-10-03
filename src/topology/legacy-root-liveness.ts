import type { MeshStateEntry } from "../mesh/store.js";

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
export const isLiveLegacyRootEntry = (entry: MeshStateEntry, now: number): entry is LegacyRootEntry => {
  if (!Number.isFinite(entry.updatedAt) || now - entry.updatedAt > LEGACY_ROOT_LEASE_MS ||
    typeof entry.value !== "object" || entry.value === null || Array.isArray(entry.value)) return false;
  const value = entry.value as Record<string, unknown>;
  return typeof value.id === "string" &&
    typeof value.sessionId === "string" && entry.key === `sessions/${value.sessionId}` &&
    entry.updatedBy?.id === value.id && typeof value.cwd === "string" &&
    typeof value.startedAt === "number" && (value.status === "idle" || value.status === "running");
};
