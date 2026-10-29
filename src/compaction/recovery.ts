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
}
const benign = (error: string | undefined): boolean => {
  const message = error?.replace(/^(?:Compaction failed|Auto-compaction failed|Context overflow recovery failed): /, "");
  return message === "Already compacted" || message === "Nothing to compact (session too small)" || message === "Compaction cancelled";
};

/** Native Pi exhausts its summary retries before this terminal notification (smarty-dev#17478). */
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
    if (event.aborted || context.signal?.aborted || benign(event.errorMessage)) return;
    const sessionId = context.sessionManager.getSessionId();
    // smarty-dev#8881: replacement can invalidate the old ctx without session_shutdown.
    if (incident?.sessionId === sessionId && extensionContextLive(incident.context)) return;
    // Latch before awaiting publication: concurrent/repeated notifications are one incident.
    incident = { sessionId, context };
    try {
      await options.alarm({ session: sessionId, windowPercent: context.getContextUsage()?.percent ?? null,
        reason: event.reason, error: detachedPreview(event.errorMessage ?? "Compaction failed", 4000),
        fabricAttempts: 0 }, context);
    } catch { /* Mesh failure must not turn a compaction error into an unhandled rejection. */ }
  });
  pi.on("session_shutdown", () => { closed = true; reset(); });
};
