import fs from "node:fs";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";

/** Native identity telemetry for disposable Pi workers, including --no-session. */
export default function runnerSession(pi: ExtensionAPI): void {
  const runId = process.env.PI_FABRIC_PARENT_RUN;
  if (!runId) return;
  let previous: string | undefined;
  const observe = (_event: unknown, ctx: ExtensionContext): void => {
    if (ctx.mode !== "rpc") return;
    const sessionId = ctx.sessionManager.getSessionId();
    if (!sessionId || sessionId === previous) return;
    previous = sessionId;
    // RPC redirects ordinary stdout writes to stderr. Use the protocol fd,
    // as activation-window does, and let the worker persist the native ID.
    fs.writeSync(1, `${JSON.stringify({ type: "fabric_runner_session", runId, sessionId })}\n`);
  };
  // session_start also fires for new/resumed/forked sessions. Checking at the
  // request boundary catches changes made by a later startup/turn extension.
  pi.on("session_start", observe);
  pi.on("agent_start", observe);
  pi.on("before_provider_request", observe);
  pi.on("session_compact", observe);
  pi.on("agent_settled", observe);
  pi.on("session_shutdown", observe);
}
