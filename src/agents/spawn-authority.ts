/** The caller identity is bound by the host runtime, never inferred from guest arguments. */
export type AgentSpawnCaller = { kind: string; id?: string } | null | undefined;

export class AgentSpawnForbiddenError extends Error {
  readonly code = "FABRIC_AGENT_SPAWN_FORBIDDEN";
  readonly launchOutcome = "unlaunched";

  constructor() {
    super("Task agents cannot spawn or hand off to additional agents or create actors");
    this.name = "AgentSpawnForbiddenError";
  }
}

const workerMarkerKeys = [
  "PI_FABRIC_PARENT_RUN",
  "PI_FABRIC_ACTOR_ID",
  "PI_FABRIC_LINEAGE",
  "PI_FABRIC_DEPTH",
  "PI_FABRIC_TASK_PROCESS_CHILD",
  "PI_FABRIC_AGENT_RUN_DIR",
  "PI_FABRIC_AGENT_NAME",
  "PI_FABRIC_SPAWNER_KIND",
  "PI_FABRIC_SPAWNER_ID",
  "PI_FABRIC_SPAWNER_RUN",
  "PI_FABRIC_SPAWNER_SESSION_ID",
  "SMARTY_ROLE",
] as const;

/** Capture launch-boundary evidence once so later process.env changes cannot alter authority. */
export const snapshotAgentSpawnEnvironment = (
  environment: NodeJS.ProcessEnv = process.env,
): NodeJS.ProcessEnv => Object.fromEntries(
  workerMarkerKeys.flatMap(key => environment[key] === undefined ? [] : [[key, environment[key]!]]),
);

export const hasWorkerMarker = (environment: NodeJS.ProcessEnv): boolean => {
  if (environment.PI_FABRIC_PARENT_RUN !== undefined) return true;
  if (environment.PI_FABRIC_ACTOR_ID !== undefined) return true;
  if (environment.PI_FABRIC_LINEAGE !== undefined) return true;
  if (environment.PI_FABRIC_DEPTH !== undefined && environment.PI_FABRIC_DEPTH !== "0") return true;
  if (environment.PI_FABRIC_TASK_PROCESS_CHILD !== undefined) return true;
  if (environment.PI_FABRIC_AGENT_RUN_DIR !== undefined) return true;
  if (environment.PI_FABRIC_AGENT_NAME !== undefined) return true;
  if (environment.PI_FABRIC_SPAWNER_KIND !== undefined) return true;
  return /^task-agent(?:@|$)/.test(environment.SMARTY_ROLE ?? "");
};

/** Refuse recursive task-agent creation before routing, launch, or registry effects. */
export const assertAgentSpawnAllowed = (
  caller: AgentSpawnCaller,
  environment: NodeJS.ProcessEnv = process.env,
): void => {
  // Minimal provider fixtures may omit the host identity. Preserve their
  // historical Main behavior, but never let a worker-marked environment use
  // that absence to bypass the boundary.
  if (caller === null) throw new AgentSpawnForbiddenError();
  if (caller === undefined) {
    if (hasWorkerMarker(environment)) throw new AgentSpawnForbiddenError();
    return;
  }
  if (caller.kind !== "main" && caller.kind !== "actor") {
    throw new AgentSpawnForbiddenError();
  }
  const actorId = environment.PI_FABRIC_ACTOR_ID?.trim();
  if (actorId && (caller.kind !== "actor" || caller.id !== actorId)) {
    throw new AgentSpawnForbiddenError();
  }
  if (hasWorkerMarker(environment)) {
    // Actor workers also carry task-shaped launch markers. Only their exact
    // host-bound actor identity grants the actor exemption, never the root Main.
    if (caller.kind !== "actor" || !actorId || caller.id !== actorId) {
      throw new AgentSpawnForbiddenError();
    }
  }
};
