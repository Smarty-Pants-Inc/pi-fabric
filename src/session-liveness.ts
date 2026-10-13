import type { ExtensionContext } from "@earendil-works/pi-coding-agent";

/**
 * smarty-dev#8881/#10718: AgentSession.dispose() and session replacement invalidate an extension
 * ctx without emitting session_shutdown to timers that captured it (pi-coding-agent docs/sdk.md),
 * and Pi exposes no dispose hook. Every getter of an invalidated ctx throws Pi's stale-ctx error.
 * Delayed work probes this before it touches the ctx and retires itself quietly when it is false.
 * Dependency-free so the eager registration graph does not grow.
 */
export const extensionContextLive = (context: ExtensionContext): boolean => {
  try { void context.hasUI; return true; } catch { return false; }
};
