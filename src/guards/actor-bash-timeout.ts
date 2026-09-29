// smarty-dev#2184: an actor run's bash call without a timeout could hang the run (and the actor's removal)
// for good. Actor runs (PI_FABRIC_ACTOR_ID set) get a default per-command timeout; an actor's
// bashTimeoutSeconds (exported as PI_FABRIC_ACTOR_BASH_TIMEOUT_S) overrides it, 0 turns it off.

export const DEFAULT_ACTOR_BASH_TIMEOUT_S = 600;


/** The timeout (seconds) to set on a bash call that has none, or undefined to leave it as is. */
export const actorBashTimeout = (
  env: Readonly<Record<string, string | undefined>>,
  timeout: unknown,
): number | undefined => {
  if (!env.PI_FABRIC_ACTOR_ID || timeout !== undefined) return undefined;
  const raw = env.PI_FABRIC_ACTOR_BASH_TIMEOUT_S;
  const seconds = raw ? Number(raw) : DEFAULT_ACTOR_BASH_TIMEOUT_S;
  if (seconds === 0) return undefined;
  return Number.isInteger(seconds) && seconds > 0 ? seconds : DEFAULT_ACTOR_BASH_TIMEOUT_S;
};
