import type { ExtensionAPI, ExtensionContext, SessionCompactFailedEvent } from "@earendil-works/pi-coding-agent";
import { extensionContextLive } from "../session-liveness.js";
import { detachedPreview } from "../core/preview.js";

export const COMPACTION_FAILED_ALARM = "fabric.session.compaction_failed";
export interface CompactionRecoveryOptions {
  enabled: () => boolean;
  alarm: (data: Record<string, unknown>, context: ExtensionContext) => Promise<void>;
  /** Unused: native Pi owns retries; a terminal failure must not signal idle or recovery. */
  retryEnded?: () => void;
}
interface Incident {
  sessionId: string;
  context: ExtensionContext;
  reasons: Set<SessionCompactFailedEvent["reason"]>;
}
const benign = (error: string | undefined): boolean => {
  const message = error?.replace(/^(?:Compaction failed|Auto-compaction failed|Context overflow recovery failed): /, "");
  return message === "Already compacted" || message === "Nothing to compact (session too small)";
};

/** Observe terminal failures only; native Pi and its summary hooks own recovery. */
export const registerCompactionRecovery = (pi: ExtensionAPI, options: CompactionRecoveryOptions): void => {
  let incident: Incident | undefined;
  let closed = false;
  const reset = (): void => {
    incident = undefined;
  };
  pi.on("session_start", () => { closed = false; reset(); });
  pi.on("session_compact", reset);
  pi.on("session_compact_failed", async (event: SessionCompactFailedEvent, context) => {
    if (!options.enabled() || closed || !extensionContextLive(context)) return;
    // Pi 05254d40 folds an extension veto into aborted, without exposing its cause.
    // Do not silence that case. A revoked context still forbids publication.
    if (context.signal?.aborted || benign(event.errorMessage)) return;
    const sessionId = context.sessionManager.getSessionId();
    // smarty-dev#8881: replacement can invalidate the old ctx without session_shutdown.
    if (incident?.sessionId === sessionId && extensionContextLive(incident.context)
      && incident.reasons.has(event.reason)) return;
    try {
      await options.alarm({ session: sessionId, windowPercent: context.getContextUsage()?.percent ?? null,
        reason: event.reason, error: detachedPreview(event.aborted || event.errorMessage?.endsWith("Compaction cancelled")
          ? "Compaction cancelled: owner cancellation or extension veto (Pi does not expose the cause)"
          : event.errorMessage ?? "Compaction failed", 4000),
        fabricAttempts: 0 }, context);
      // No in-flight lock: concurrent observations may both publish. Only a
      // resolved publication latches, and never on behalf of a replaced session.
      if (closed || !extensionContextLive(context) || context.signal?.aborted
        || context.sessionManager.getSessionId() !== sessionId) return;
      if (incident?.sessionId === sessionId && extensionContextLive(incident.context)) {
        incident.reasons.add(event.reason);
      } else {
        incident = { sessionId, context, reasons: new Set([event.reason]) };
      }
    } catch { /* Mesh failure must not turn a compaction error into an unhandled rejection. */ }
  });
  pi.on("session_shutdown", () => { closed = true; reset(); });
};
