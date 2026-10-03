/** Shared display/lookup name rule for root sessions and actor participants. Names are not authority. */
export const PARTICIPANT_NAME_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9 _.-]{0,59}$/;

/** Explicit Pi names override the launch agent name; completion return addresses must match presence. */
export const rootParticipantName = (
  sessionName?: string,
  environment: NodeJS.ProcessEnv = process.env,
): string => {
  const name = sessionName?.trim();
  if (name && PARTICIPANT_NAME_PATTERN.test(name)) return name;
  // SMARTY_ROLE is agent@revision; PI_FABRIC_ROLE is topology metadata, not the agent name.
  const agent = environment.SMARTY_ROLE?.split("@")[0]?.trim();
  return agent && PARTICIPANT_NAME_PATTERN.test(agent) ? agent : "main";
};
