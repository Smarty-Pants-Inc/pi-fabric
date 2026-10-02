/** Task identity is set before exec, not by mutating the worker's environment later. */
export const taskAgentEnvironment = (environment: NodeJS.ProcessEnv = process.env): NodeJS.ProcessEnv => {
  const child = { ...environment };
  // smarty-role's @stamp is the shared org repository revision, not a role
  // content hash. Keep that provenance when switching to its task-agent role;
  // an unstamped/non-fleet parent must not invent a revision.
  const revision = environment.SMARTY_ROLE?.match(/@([0-9a-f]{12,40})$/)?.[1];
  child.SMARTY_ROLE = `task-agent${revision ? `@${revision}` : ""}`;
  // participantRole prefers this override; leaving it inherited hides the
  // child's task role. A parent's critical-read class is role-specific too.
  delete child.PI_FABRIC_ROLE;
  delete child.SMARTY_READ_CLASS;
  return child;
};
