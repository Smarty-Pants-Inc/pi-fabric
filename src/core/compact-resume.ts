import { writeSync } from "node:fs";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { CompactLastCommit, CompactPendingIntent } from "./compact-controller.js";
import { COMPACT_RESUME_ENTRY_TYPE as ENTRY_TYPE, compactResumeMessageIds } from "../compaction/resume-delivery.js";

interface ResumeEntry { id: string; resume: string; state: "pending" | "cancelled"; reason?: string }
export const LIVE_COMPACT_RESUME_REFUSAL = "automatic compaction resume disabled: re-submit pending work explicitly (smarty-dev#5282)";
export const RESTART_COMPACT_RESUME_REFUSAL = "restart recovery refused: original admission cannot be proven";

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

export const reportCompactResumeRefusal = (context: ExtensionContext, count: number, startup = false): void => {
  if (!count) return;
  const message = startup
    ? "Compaction restart recovery refused: original admission cannot be proven. Automatic resume is disabled (smarty-dev#5282); re-submit the pending work explicitly."
    : "Automatic compaction resume is disabled (smarty-dev#5282). Re-submit the pending work explicitly.";
  if (context.hasUI) context.ui.notify(message, "warning");
  if (context.mode === "rpc") {
    writeSync(1, `${JSON.stringify({ type: "fabric_compact_resume_refused", protocol: 1,
      runId: process.env.PI_FABRIC_PARENT_RUN, count, message })}\n`);
  }
};

// Journal immediately before compact starts, not when the replaceable intent is
// requested. If the process dies mid-compaction, startup retires this entry.
export const beginCompactResume = (pi: ExtensionAPI, intent: CompactPendingIntent): void => {
  if (intent.resume && intent.resumeId) {
    pi.appendEntry<ResumeEntry>(ENTRY_TYPE, { id: intent.resumeId, resume: intent.resume, state: "pending" });
  }
};

export const settleCompactResume = (
  pi: ExtensionAPI, intent: CompactPendingIntent, _status: CompactLastCommit["status"], context: ExtensionContext,
): void => {
  if (!intent.resume || !intent.resumeId) return;
  // Refuse every outcome, including successful compaction after an explicit
  // abort. At agent_settled native Pi may already have cleared context.signal;
  // neither that signal nor a successful compaction authorizes a fresh turn.
  // Also journal a pre-start abort, where onBegin never ran.
  pi.appendEntry<ResumeEntry>(ENTRY_TYPE, { id: intent.resumeId, resume: intent.resume, state: "cancelled",
    reason: LIVE_COMPACT_RESUME_REFUSAL });
  reportCompactResumeRefusal(context, 1);
};

// The safe floor never replays a continuation, live or recovered. Retire all
// pending journal entries before new input, regardless of whether compaction
// committed. Already admitted legacy receipts remain untouched.
export const recoverCompactResume = (
  pi: ExtensionAPI, context: ExtensionContext, startup = false,
): number => {
  const pending = new Map<string, ResumeEntry>();
  const messages = new Set<string>();
  for (const entry of context.sessionManager?.getBranch?.() ?? []) {
    if (entry.type === "custom" && entry.customType === ENTRY_TYPE) {
      const data = entry.data as Partial<ResumeEntry> | undefined;
      if (!data || typeof data.id !== "string" || typeof data.resume !== "string" ||
        !data.resume.trim() || data.resume.length > 16_384) continue;
      if (data.state === "pending") pending.set(data.id, data as ResumeEntry);
      else if (data.state === "cancelled") pending.delete(data.id);
    } else if (entry.type === "message" && entry.message.role === "user") {
      const content = entry.message.content;
      const text = typeof content === "string" ? content : content
        .filter(part => part.type === "text").map(part => part.text).join("\n");
      for (const receipt of compactResumeMessageIds(text)) messages.add(receipt);
    }
  }
  let refused = 0;
  for (const [id, intent] of pending) {
    if (messages.has(id)) continue;
    pi.appendEntry<ResumeEntry>(ENTRY_TYPE, { ...intent, state: "cancelled",
      reason: startup ? RESTART_COMPACT_RESUME_REFUSAL : LIVE_COMPACT_RESUME_REFUSAL });
    refused++;
  }
  return refused;
};
