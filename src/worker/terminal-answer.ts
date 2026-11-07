import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { readFileRetrying, writeFileAtomic, withExclusiveFileLock } from "../core/atomic-write.js";

/** Content-free native evidence travels with the durable receipt, not just IPC. */
export interface AssistantAnswerEvidence {
  lastAssistantStopReason?: string | undefined;
  lastAssistantBlockCounts: Record<string, number>;
  lastAssistantTextBytes: number;
  unresolvedToolCalls: number;
}

export const assistantAnswerEvidence = (message: unknown, unresolvedToolCalls: number): AssistantAnswerEvidence => {
  const value = message as { stopReason?: unknown; content?: unknown } | undefined;
  const counts: Record<string, number> = {};
  let text = "";
  if (typeof value?.content === "string") { counts.text = 1; text = value.content; }
  else if (Array.isArray(value?.content)) {
    for (const block of value.content) {
      if (typeof block?.type !== "string") continue;
      Object.defineProperty(counts, block.type, { value: (Object.hasOwn(counts, block.type) ? counts[block.type]! : 0) + 1,
        enumerable: true, configurable: true, writable: true });
      if (block.type === "text" && typeof block.text === "string") text += block.text;
    }
  }
  return { lastAssistantStopReason: typeof value?.stopReason === "string" ? value.stopReason : undefined,
    lastAssistantBlockCounts: counts, lastAssistantTextBytes: Buffer.byteLength(text, "utf8"), unresolvedToolCalls };
};

export interface FinalAnswerReceipt {
  version: 1;
  id: string;
  runId: string;
  recordedAt: number;
  text: string;
  /** Optional only for receipts from older workers. Never infer a stop from text. */
  assistant?: AssistantAnswerEvidence;
  /** Only the native successful fabric_reply path writes this marker. */
  structuredReply?: true;
}

export const readFinalAnswerReceipt = (runDirectory: string, runId: string): FinalAnswerReceipt | undefined => {
  try {
    const value = JSON.parse(readFileRetrying(path.join(runDirectory, "final-answer.json")));
    if (value?.version === 1 && value.runId === runId && typeof value.id === "string" && value.id.length > 0 &&
        Number.isSafeInteger(value.recordedAt) && value.recordedAt > 0 && typeof value.text === "string") return value;
  } catch { /* Missing or invalid receipts never establish a terminal boundary. */ }
  return undefined;
};

/** Publication and stdin injection share one run-local cross-process ordering. */
export const withTaskFinalAnswerLock = <T>(runDirectory: string, operation: () => T): T => withExclusiveFileLock({
  directory: runDirectory, lockName: ".task-final-answer.lock", attempts: 200, delayMs: 5,
  timeoutMessage: "Task final-answer publication/injection lock timed out",
}, operation);

/** One native writer owns this run. A retry/late frame can never replace its answer. */
export const saveFinalAnswerReceipt = (runDirectory: string, runId: string, text: string,
  evidence: Pick<FinalAnswerReceipt, "assistant" | "structuredReply"> = {}): FinalAnswerReceipt => withTaskFinalAnswerLock(runDirectory, () => {
  const existing = readFinalAnswerReceipt(runDirectory, runId);
  if (existing) return existing;
  const file = path.join(runDirectory, "final-answer.json");
  if (fs.existsSync(file)) throw new Error("Invalid or mismatched final-answer receipt already exists");
  const receipt: FinalAnswerReceipt = { version: 1, id: randomUUID(), runId, recordedAt: Date.now(), text, ...evidence };
  writeFileAtomic(file, JSON.stringify(receipt), { durable: true });
  return receipt;
});

/** Text alongside tool calls is progress, not a final answer; truncation/retry isn't success. */
export const finalAssistantText = (message: unknown): string | undefined => {
  if (!message || typeof message !== "object") return undefined;
  const value = message as { role?: unknown; stopReason?: unknown; content?: unknown };
  if (value.role !== "assistant" || value.stopReason !== "stop" || !Array.isArray(value.content) ||
      value.content.some(block => block?.type === "toolCall")) return undefined;
  const text = value.content.filter(block => block?.type === "text" && typeof block.text === "string")
    .map(block => block.text).join("");
  return text.trim().length > 0 ? text : undefined;
};
