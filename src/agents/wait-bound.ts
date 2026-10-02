import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { FOREGROUND_WAIT_LIMIT_S } from "../guards/foreground-wait.js";
import { resolveFabricIdentity } from "../fabric-provenance.js";

// smarty-dev#854: a wait without a bound blocked its session for over an hour. agents.wait and
// agents.join (native, durable and hosted) take timeoutMs, 5 minutes by default. Every wait runs
// inside a fabric_exec tool call, which holds its session in the foreground, so a larger timeoutMs
// is clamped to the bash guard's 5-minute limit: a 60-minute cap still let waits hold sessions for
// 20 min (acceptance audit on #854). At the bound only the wait ends; the child keeps running and
// its result arrives as a completion message after the turn.
export const AGENT_WAIT_DEFAULT_MS = 5 * 60 * 1_000;
export const AGENT_WAIT_MAX_MS = FOREGROUND_WAIT_LIMIT_S * 1_000;
// smarty-dev#2119: an interactive Main is the session people and leads steer. Its waits end within
// 60 s and return the live status, so the caller is back at a tool boundary where held followUps land.
export const MAIN_AGENT_WAIT_MAX_MS = 60 * 1_000;

/** The bound for one wait: the caller's timeoutMs, clamped to max, or the default. */
export const agentWaitBound = (timeoutMs: unknown, max = AGENT_WAIT_MAX_MS): number =>
  Math.min(max, Math.max(1_000, typeof timeoutMs === "number" && Number.isFinite(timeoutMs)
    ? Math.floor(timeoutMs) : AGENT_WAIT_DEFAULT_MS));

/**
 * Leave time for a Main observation result and guest continuation before the fixed program ceiling.
 * Compute at observation start (after launch), not from a fresh per-call program duration. Below
 * the 1 s observation floor the program ceiling still wins with its named error; do not busy-spin.
 */
export const mainAgentWaitBound = (timeoutMs: unknown, mainDeadlineAt?: number): number =>
  Math.min(
    agentWaitBound(timeoutMs, MAIN_AGENT_WAIT_MAX_MS),
    Math.max(1_000, (mainDeadlineAt ?? Infinity) - Date.now() - 2_000),
  );

/**
 * An interactive Main (TUI or RPC, not a task agent or actor): its waits use MAIN_AGENT_WAIT_MAX_MS
 * and return at the bound instead of throwing. Print and JSON runs are scripts and keep the 5 min.
 */
export const isInteractiveMain = (
  context: Pick<ExtensionContext, "mode" | "sessionManager"> | undefined,
  environment: NodeJS.ProcessEnv = process.env,
): boolean => {
  if (context?.mode !== "tui" && context?.mode !== "rpc") return false;
  if (typeof context.sessionManager?.getSessionId !== "function") return false;
  return resolveFabricIdentity(context.sessionManager.getSessionId(), environment).identity.kind === "main";
};

/** A wait reached its bound; the agent keeps running. */
export class AgentWaitBoundError extends Error {
  override readonly name = "AgentWaitBoundError";
}

/** A wait bound for a message: "0.3 s", "5 min". */
export const describeWaitBound = (ms: number): string =>
  ms < 60_000 ? `${Math.round(ms / 100) / 10} s` : `${Math.round(ms / 6_000) / 10} min`;
