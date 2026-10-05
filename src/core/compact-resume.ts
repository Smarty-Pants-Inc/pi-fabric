import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { CompactLastCommit, CompactPendingIntent } from "./compact-controller.js";
import { COMPACT_RESUME_ENTRY_TYPE as ENTRY_TYPE, compactResumeMessage as resumeMessage, compactResumeMessageIds } from "../compaction/resume-delivery.js";

interface ResumeEntry { id: string; resume: string; state: "pending" | "cancelled"; reason?: string }
const scheduled = new WeakMap<ExtensionAPI, Set<string>>();
// A journal alone cannot establish the original sender. Only intents begun by
// this extension instance may resume in the live session; restart/reload cannot
// borrow the next input's principal. No startup input transform is installed.
const live = new WeakMap<ExtensionAPI, Set<string>>();

// Only an explicit compound compact directive is inferred. Arbitrary old tasks,
// summary-preservation instructions, and a plain /compact must not wake a turn.
export const inferCompactResume = (context: ExtensionContext): string | undefined => {
  const branch = context.sessionManager?.getBranch?.() ?? [];
  for (let i = branch.length - 1; i >= 0; i--) {
    const entry = branch[i]!;
    if (entry.type !== "message" || entry.message.role !== "user") continue;
    const content = entry.message.content;
    const text = typeof content === "string" ? content : content
      .filter(part => part.type === "text").map(part => part.text).join("\n");
    // Worker-owned followUps retain their transport ID in the session log.
    // Keep this exact envelope aligned with agents/follow-up-delivery.ts.
    const instruction = text.replace(/^\[fabric-follow-up:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\]\n/, "");
    const match = instruction.match(/^\s*(?:please\s+)?\/?compact(?:\s+(?:first|now|your context|the context|context|this session))*\s*(?:[,;:.!]\s*)?(?:(?:and\s+)?then\s+|and\s+|afterwards[,\s]+)([\s\S]+)$/i);
    const resume = match?.[1]?.trim();
    return resume && resume.length <= 16_384 ? resume : undefined;
  }
  return undefined;
};

// Write immediately before compact starts, not when the replaceable intent is
// requested. Cancelled/replaced intents therefore cannot become stale wakeups.
export const beginCompactResume = (pi: ExtensionAPI, intent: CompactPendingIntent): void => {
  if (intent.resume && intent.resumeId) {
    let owned = live.get(pi);
    if (!owned) { owned = new Set(); live.set(pi, owned); }
    owned.add(intent.resumeId);
    pi.appendEntry<ResumeEntry>(ENTRY_TYPE, { id: intent.resumeId, resume: intent.resume, state: "pending" });
  }
};

export const settleCompactResume = (
  pi: ExtensionAPI, intent: CompactPendingIntent, status: CompactLastCommit["status"], context: ExtensionContext,
): void => {
  if (!intent.resume || !intent.resumeId) return;
  if (status !== "committed") {
    pi.appendEntry<ResumeEntry>(ENTRY_TYPE, { id: intent.resumeId, resume: intent.resume, state: "cancelled" });
    return;
  }
  recoverCompactResume(pi, context);
};

// A successful compaction entry is the durable commit witness. The delivered
// user message is the durable receipt, rather than a pre-delivery claim that
// could lose the continuation on a crash. The process-local set only closes
// the gap while Pi defers sendUserMessage past its remaining settled handlers.
export const recoverCompactResume = (
  pi: ExtensionAPI, context: ExtensionContext, startup = false,
): number => {
  const pending = new Map<string, ResumeEntry>();
  const ready = new Set<string>();
  const messages = new Set<string>();
  for (const entry of context.sessionManager?.getBranch?.() ?? []) {
    if (entry.type === "custom" && entry.customType === ENTRY_TYPE) {
      const data = entry.data as Partial<ResumeEntry> | undefined;
      if (!data || typeof data.id !== "string" || typeof data.resume !== "string" ||
        !data.resume.trim() || data.resume.length > 16_384) continue;
      if (data.state === "pending") pending.set(data.id, data as ResumeEntry);
      else if (data.state === "cancelled") { pending.delete(data.id); ready.delete(data.id); }
    } else if (entry.type === "compaction") {
      for (const id of pending.keys()) ready.add(id);
    } else if (entry.type === "message" && entry.message.role === "user") {
      const content = entry.message.content;
      const text = typeof content === "string" ? content : content
        .filter(part => part.type === "text").map(part => part.text).join("\n");
      for (const receipt of compactResumeMessageIds(text)) messages.add(receipt);
    }
  }
  // Restart recovery is deliberately refused until the journal can establish
  // original admission AND the parent's cumulative output lineage. Persist the
  // refusal before any new input; neither committed nor pre-commit work may be
  // revived by a later compaction. Already admitted receipts remain untouched.
  if (startup) {
    live.delete(pi);
    let refused = 0;
    for (const [id, intent] of pending) {
      if (messages.has(id)) continue;
      pi.appendEntry<ResumeEntry>(ENTRY_TYPE, { ...intent, state: "cancelled",
        reason: "restart recovery refused: original admission cannot be proven" });
      refused++;
    }
    return refused;
  }
  let sent = scheduled.get(pi);
  if (!sent) { sent = new Set(); scheduled.set(pi, sent); }
  for (const id of ready) {
    const intent = pending.get(id)!;
    const text = resumeMessage(intent);
    const key = `${context.sessionManager.getSessionId()}:${id}`;
    if (!live.get(pi)?.has(id) || messages.has(id) || sent.has(key)) continue;
    sent.add(key);
    try {
      pi.sendUserMessage(text, { deliverAs: "followUp" });
    } catch (error) {
      sent.delete(key);
      throw error;
    }
  }
  return 0;
};
