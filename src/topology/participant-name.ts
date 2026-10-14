/** Shared display/lookup name rule for root sessions and actor participants. Names are not authority. */
export const PARTICIPANT_NAME_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9 _.-]{0,59}$/;

export const validName = (name?: string): string | undefined => {
  const trimmed = name?.trim();
  return trimmed && PARTICIPANT_NAME_PATTERN.test(trimmed) ? trimmed : undefined;
};

/**
 * The root name presence publishes for a Pi session name; completion return addresses must match it.
 * A valid Pi session name wins; else the interactive Main's SMARTY_AGENT launch name
 * (smarty-dev#11077); else its Herdr agent name (smarty-dev#6758); else "main".
 */
export const rootParticipantName = (sessionName?: string, herdrName?: string, smartyAgent?: string): string =>
  validName(sessionName) ?? validName(smartyAgent) ?? validName(herdrName) ?? "main";
