import type { AgentTransportLaunch } from "../types.js";

/** No async gap is allowed between this check and the worker-creation side effect. */
export const assertTransportLaunchAllowed = (
  request?: Pick<AgentTransportLaunch, "signal" | "authorize">,
): void => {
  if (request?.signal?.aborted) throw new Error("Agent launch aborted");
  if (request?.authorize && !request.authorize()) throw new Error("Agent activation no longer authorized");
};
