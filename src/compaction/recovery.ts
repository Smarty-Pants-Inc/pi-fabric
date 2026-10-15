import type { ExtensionAPI, ExtensionContext, SessionCompactFailedEvent } from "@earendil-works/pi-coding-agent";
import { extensionContextLive } from "../session-liveness.js";
import { detachedPreview } from "../core/preview.js";

export const COMPACTION_FAILED_ALARM = "fabric.session.compaction_failed";
export interface CompactionRecoveryOptions {
  enabled: () => boolean;
  alarm: (data: Record<string, unknown>, context: ExtensionContext) => Promise<void>;
  /** The retry compaction ended, either way, and Pi has left its compaction state (smarty-dev#9271). */
  retryEnded?: () => void;
}
interface Incident {
  sessionId: string;
  context: ExtensionContext;
  reason: string;
  customInstructions?: string | undefined;
  windowPercent: number | null;
  firstError: string;
  retrying: boolean;
  alarmed: boolean;
}
const benign = (error: string | undefined): boolean => {
  const message = error?.replace(/^(?:Compaction failed|Auto-compaction failed|Context overflow recovery failed): /, "");
  return message === "Already compacted" || message === "Nothing to compact (session too small)" || message === "Compaction cancelled";
};

/** Public ctx.compact aborts an active run: defer, never await it inside a Pi failure hook. */
export const registerCompactionRecovery = (pi: ExtensionAPI, options: CompactionRecoveryOptions): void => {
  let incident: Incident | undefined;
  let instructions: string | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let closed = false;
  const reset = (): void => {
    incident = undefined;
    if (timer) clearTimeout(timer);
    timer = undefined;
  };
  // smarty-dev#8881: dispose/replacement invalidates the ctx with no session_shutdown. Retire the
  // incident quietly instead of touching it, so a later failure on a live ctx retries afresh.
  const retired = (current: Incident): boolean => {
    if (extensionContextLive(current.context)) return false;
    if (incident === current) reset();
    return true;
  };
  const alarm = async (current: Incident, error: string): Promise<void> => {
    if (closed || incident !== current || current.alarmed || retired(current)) return;
    current.alarmed = true;
    try {
      await options.alarm({ session: current.sessionId, windowPercent: current.windowPercent,
        reason: current.reason, error: detachedPreview(error, 4000), firstError: detachedPreview(current.firstError, 4000), attempts: 2 }, current.context);
    } catch { /* Mesh failure must not turn a compaction error into an unhandled rejection. */ }
  };
  const schedule = (context: ExtensionContext): void => {
    if (!incident || incident.retrying || timer || closed) return;
    incident.context = context;
    // Exit the emitting hook first. Even a failure reporting idle can still own Pi's compaction stack.
    timer = setTimeout(() => {
      timer = undefined;
      const current = incident;
      if (!current || current.retrying || closed) return;
      if (retired(current)) return;
      const ctx = current.context;
      if (ctx.sessionManager.getSessionId() !== current.sessionId || ctx.signal?.aborted) { reset(); return; }
      if (!ctx.isIdle()) return; // agent_settled supplies the next safe boundary, without polling.
      current.retrying = true;
      try {
        ctx.compact({ ...(current.customInstructions ? { customInstructions: current.customInstructions } : {}),
          onComplete: () => { if (retired(current)) return; if (incident === current) reset(); options.retryEnded?.(); },
          onError: error => { if (retired(current)) return; void alarm(current, error.message); options.retryEnded?.(); },
        });
      } catch (error) {
        void alarm(current, error instanceof Error ? error.message : String(error));
      }
    }, 0);
    timer.unref?.();
  };
  pi.on("session_start", () => { closed = false; reset(); instructions = undefined; });
  pi.on("session_before_compact", event => { instructions = event.customInstructions; });
  pi.on("session_compact", () => { reset(); instructions = undefined; });
  pi.on("session_compact_failed", async (event: SessionCompactFailedEvent, context) => {
    if (!options.enabled() || closed) return;
    if (event.aborted || context.signal?.aborted || benign(event.errorMessage)) { reset(); return; }
    if (incident?.retrying) { await alarm(incident, event.errorMessage ?? "Compaction failed"); return; }
    if (!incident) {
      const usage = context.getContextUsage();
      incident = { sessionId: context.sessionManager.getSessionId(), context, reason: event.reason,
        customInstructions: instructions, windowPercent: usage?.percent ?? null,
        firstError: event.errorMessage ?? "Compaction failed", retrying: false, alarmed: false };
    }
    schedule(context);
  });
  pi.on("agent_settled", (_event, context) => { schedule(context); });
  pi.on("session_shutdown", () => { closed = true; reset(); });
};
