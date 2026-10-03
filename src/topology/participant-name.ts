/** Shared display/lookup name rule for root sessions and actor participants. Names are not authority. */
export const PARTICIPANT_NAME_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9 _.-]{0,59}$/;

/** The root name presence publishes for a Pi session name; completion return addresses must match it. */
export const rootParticipantName = (sessionName?: string): string => {
  const name = sessionName?.trim();
  return name && PARTICIPANT_NAME_PATTERN.test(name) ? name : "main";
};
