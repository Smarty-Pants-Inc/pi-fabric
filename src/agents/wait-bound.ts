import { FOREGROUND_WAIT_LIMIT_S } from "../guards/foreground-wait.js";

// smarty-dev#854: a wait without a bound blocked its session for over an hour. agents.wait and
// agents.join (native, durable and hosted) take timeoutMs, 5 minutes by default. Every wait runs
// inside a fabric_exec tool call, which holds its session in the foreground, so a larger timeoutMs
// is clamped to the bash guard's 5-minute limit: a 60-minute cap still let waits hold sessions for
// 20 min (acceptance audit on #854). At the bound only the wait ends; the child keeps running and
// its result arrives as a completion message after the turn.
export const AGENT_WAIT_DEFAULT_MS = 5 * 60 * 1_000;
export const AGENT_WAIT_MAX_MS = FOREGROUND_WAIT_LIMIT_S * 1_000;

/** The bound for one wait: the caller's timeoutMs, clamped, or the default. */
export const agentWaitBound = (timeoutMs: unknown): number =>
  Math.min(AGENT_WAIT_MAX_MS, Math.max(1_000, typeof timeoutMs === "number" && Number.isFinite(timeoutMs)
    ? Math.floor(timeoutMs) : AGENT_WAIT_DEFAULT_MS));

/** A wait bound for a message: "0.3 s", "5 min". */
export const describeWaitBound = (ms: number): string =>
  ms < 60_000 ? `${Math.round(ms / 100) / 10} s` : `${Math.round(ms / 6_000) / 10} min`;
