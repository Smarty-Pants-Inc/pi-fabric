import type { ExtensionContext } from "@earendil-works/pi-coding-agent";

// A run the user aborted, or one that failed, must not start another turn by itself. Newer Pi
// names the outcome; for older Pi, the last assistant message's stop reason says it.
export const settledCompleted = (event: unknown, context: ExtensionContext): boolean => {
  const outcome = (event as { outcome?: unknown }).outcome;
  if (typeof outcome === "string") return outcome === "completed";
  if (context.signal?.aborted) return false;
  const entries = context.sessionManager.getEntries();
  for (let index = entries.length - 1; index >= Math.max(0, entries.length - 50); index--) {
    const entry = entries[index] as { type?: string; message?: { role?: string; stopReason?: string } };
    if (entry.type === "message" && entry.message?.role === "assistant") {
      return entry.message.stopReason !== "aborted" && entry.message.stopReason !== "error";
    }
  }
  return true;
};
