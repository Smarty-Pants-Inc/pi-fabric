/** A new run cannot claim its caller's task boundary; keep roles and ordinary env intact. */
export const withoutTaskBoundary = (environment: NodeJS.ProcessEnv): NodeJS.ProcessEnv => {
  const child = { ...environment };
  delete child.FABRIC_TASK_MEMORY_MAX;
  delete child.FABRIC_TASK_CGROUP;
  delete child.FABRIC_TASK_SYSTEMD_RUN_WRAPPER;
  return child;
};

/**
 * Task identity is set before exec, not by mutating the worker's environment later.
 * `preserveTaskBoundary` is only for the worker's own same-run child: the launcher exported the
 * FABRIC_TASK_* boundary evidence into this worker after admitting its capped receipt, so the
 * native agent of the same run keeps it. A new task launch (default) never inherits it.
 */
export const taskAgentEnvironment = (
  environment: NodeJS.ProcessEnv = process.env,
  preserveTaskBoundary = false,
): NodeJS.ProcessEnv => {
  const child = preserveTaskBoundary ? { ...environment } : withoutTaskBoundary(environment);
  // smarty-role's @stamp is the shared org repository revision, not a role
  // content hash. Keep that provenance when switching to its task-agent role;
  // an unstamped/non-fleet parent must not invent a revision.
  const revision = environment.SMARTY_ROLE?.match(/@([0-9a-f]{12,40})$/)?.[1];
  child.SMARTY_ROLE = `task-agent${revision ? `@${revision}` : ""}`;
  // participantRole prefers this override; leaving it inherited hides the
  // child's task role. A parent's critical-read class is role-specific too.
  delete child.PI_FABRIC_ROLE;
  // The task role is a new launch grant, never the parent root's session/project binding.
  delete child.PI_FABRIC_ROLE_SESSION;
  delete child.PI_FABRIC_ROLE_PROJECT;
  delete child.SMARTY_READ_CLASS;
  // An actor's task child is a task, not another activation of the parent's actor.
  delete child.PI_FABRIC_ACTOR_ID;
  delete child.PI_FABRIC_TASK_PROCESS_CHILD;
  return child;
};
