/** Shared display/lookup name rule for root sessions and actor participants. Names are not authority. */
export const PARTICIPANT_NAME_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9 _.-]{0,59}$/;

/** Exact smarty-role validate_agent_name contract; unlike Pi session names, no trimming or spaces. */
// (?![\s\S]) requires the actual end: JS $ alone also accepts a final newline.
const SMARTY_AGENT_NAME_PATTERN = /^[A-Za-z0-9_-][A-Za-z0-9._-]{0,63}(?![\s\S])/;

/** Launch agent names override explicit Pi names only in a root Main. */
export const rootParticipantName = (
  sessionName?: string,
  environment: NodeJS.ProcessEnv = process.env,
): string => {
  // SMARTY_ROLE is a role-template@revision stamp, never the agent's published name.
  // Defend even against older/external child launchers that retain root metadata.
  const child = environment.PI_FABRIC_PARENT_RUN?.trim() || environment.PI_FABRIC_ACTOR_ID?.trim()
    || environment.PI_FABRIC_TASK_PROCESS_CHILD === "1" || Number(environment.PI_FABRIC_DEPTH) > 0;
  const agent = environment.SMARTY_AGENT_NAME;
  if (!child && agent && SMARTY_AGENT_NAME_PATTERN.test(agent)) return agent;
  const name = sessionName?.trim();
  return name && PARTICIPANT_NAME_PATTERN.test(name) ? name : "main";
};
