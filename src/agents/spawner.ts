import type { AgentSpawner } from "./types.js";

/** Snapshot the caller, not the caller's inherited lineage Main. */
export const resolveAgentSpawner = (
  identityId?: string,
  mainAgentId?: string,
  environment: NodeJS.ProcessEnv = process.env,
): AgentSpawner | undefined => {
  const actorId = environment.PI_FABRIC_ACTOR_ID?.trim();
  const runId = environment.PI_FABRIC_PARENT_RUN?.trim();
  if (actorId) return { id: actorId, kind: "actor", ...(runId ? { runId } : {}) };
  if (runId) return { id: runId, kind: "agent", runId };
  // Resident hosts execute Main-owned launches on Main's behalf, not as a public spawner.
  const id = mainAgentId ?? identityId;
  return id ? { id, kind: "main" } : undefined;
};

/** A child's bound reply target. No root fallback: older workers have no binding. */
export const boundAgentSpawner = (
  environment: NodeJS.ProcessEnv = process.env,
): AgentSpawner | undefined => {
  const id = environment.PI_FABRIC_SPAWNER_ID?.trim();
  const kind = environment.PI_FABRIC_SPAWNER_KIND?.trim();
  const runId = environment.PI_FABRIC_SPAWNER_RUN?.trim();
  if (!id || (kind !== "main" && kind !== "agent" && kind !== "actor")) return undefined;
  return { id, kind, ...(runId ? { runId } : {}) };
};
