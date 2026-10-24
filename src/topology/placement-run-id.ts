const RUN_ID = /^[a-z0-9](?:[a-z0-9-]{0,55}[a-z0-9])?$/u;

export const isPlacementRunId = (value: unknown): value is string => typeof value === "string" && RUN_ID.test(value);

/** Pod-injected placement identity, copied only onto the host-local root presence record. */
export const placementRunIdForParticipant = (kind: string, env: NodeJS.ProcessEnv = process.env): string | undefined => {
  if (kind !== "root") return undefined;
  const runId = env.PI_FABRIC_RUN_ID;
  return isPlacementRunId(runId) ? runId : undefined;
};
