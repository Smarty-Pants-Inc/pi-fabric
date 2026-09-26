// smarty-dev#854: a wait without a bound blocked its session for over an hour. agents.wait and
// agents.join (native, durable and hosted) take timeoutMs, 5 minutes by default and at most 60.
export const AGENT_WAIT_DEFAULT_MS = 5 * 60 * 1_000;
export const AGENT_WAIT_MAX_MS = 60 * 60 * 1_000;

/** The bound for one wait: the caller's timeoutMs, clamped, or the default. */
export const agentWaitBound = (timeoutMs: unknown): number =>
  Math.min(AGENT_WAIT_MAX_MS, Math.max(1_000, typeof timeoutMs === "number" && Number.isFinite(timeoutMs)
    ? Math.floor(timeoutMs) : AGENT_WAIT_DEFAULT_MS));

/** A wait bound for a message: "0.3 s", "5 min". */
export const describeWaitBound = (ms: number): string =>
  ms < 60_000 ? `${Math.round(ms / 100) / 10} s` : `${Math.round(ms / 6_000) / 10} min`;
