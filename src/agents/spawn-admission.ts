/**
 * Leaf-by-default task spawn admission (smarty-dev#12187).
 *
 * A Fabric task agent (identity kind "agent": PI_FABRIC_PARENT_RUN without
 * PI_FABRIC_ACTOR_ID) may not create further agents unless its own launch was
 * `recursive: true`. That launch grant is the manager's `--granted-risks
 * ["agent"]`, which worker.ts writes into PI_FABRIC_GRANTED_RISKS, replacing
 * any inherited value. The decision is snapshotted once, host-side, so a task
 * cannot grant itself spawning by mutating its environment later, by asking
 * for `recursive: true` on its own child, or by any routed/durable/model:auto
 * or generic tools.call path: all of them reach the provider admission gate.
 * Main and actors (actor identity wins even when PI_FABRIC_PARENT_RUN is set)
 * are unaffected. Keep this module dependency-free.
 */

export const TASK_SPAWN_DENIED_CODE = "FABRIC_TASK_SPAWN_DENIED";
export const TASK_SPAWN_DENIED_RULE = "task agents do not spawn; return your result to your spawner";

/** Fixed, guest-visible refusal: name and code are allowlisted by the QuickJS error bridge. */
export class TaskSpawnDeniedError extends Error {
  readonly code = TASK_SPAWN_DENIED_CODE;
  constructor(action: string, taskId: string) {
    super(`${TASK_SPAWN_DENIED_RULE} (agents.${action} refused for task ${taskId}; ` +
      "launch the task with recursive: true to grant spawning).");
    this.name = "TaskSpawnDeniedError";
  }
}

export interface TaskSpawnAdmission {
  readonly kind: "main" | "agent" | "actor";
  /** Task run id for kind "agent". */
  readonly id?: string;
  /** True for Main, actors, and tasks launched `recursive: true`. */
  readonly spawnAllowed: boolean;
}

/** Capture the caller's admission from its launch environment. The result is frozen. */
export const captureTaskSpawnAdmission = (environment: NodeJS.ProcessEnv = process.env): TaskSpawnAdmission => {
  // Actor identity has priority, matching resolveFabricIdentity and resolveAgentSpawner.
  if (environment.PI_FABRIC_ACTOR_ID?.trim()) return Object.freeze({ kind: "actor", spawnAllowed: true });
  const runId = environment.PI_FABRIC_PARENT_RUN?.trim();
  if (!runId) return Object.freeze({ kind: "main", spawnAllowed: true });
  const granted = (environment.PI_FABRIC_GRANTED_RISKS ?? "").split(",").map((risk) => risk.trim());
  return Object.freeze({ kind: "agent", id: runId, spawnAllowed: granted.includes("agent") });
};

/** Throws the fixed TaskSpawnDeniedError for a non-recursive task agent. */
export const assertTaskSpawnAdmitted = (admission: TaskSpawnAdmission, action: string): void => {
  if (admission.spawnAllowed) return;
  throw new TaskSpawnDeniedError(action, admission.id ?? "unknown");
};
