/** Shared display/lookup name rule for root sessions and actor participants. Names are not authority. */
export const PARTICIPANT_NAME_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9 _.-]{0,59}$/;

/** Launch agent names override explicit Pi names; completion return addresses must match presence. */
export const rootParticipantName = (
  sessionName?: string,
  environment: NodeJS.ProcessEnv = process.env,
): string => {
  // SMARTY_ROLE is a role-template@revision stamp, never the agent's published name.
  const agent = environment.SMARTY_AGENT_NAME?.trim();
  if (agent && PARTICIPANT_NAME_PATTERN.test(agent)) return agent;
  const name = sessionName?.trim();
  return name && PARTICIPANT_NAME_PATTERN.test(name) ? name : "main";
};
