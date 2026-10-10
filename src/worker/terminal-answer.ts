import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { readFileRetrying, writeFileAtomic } from "../core/atomic-write.js";

export interface FinalAnswerReceipt {
  version: 1;
  id: string;
  runId: string;
  recordedAt: number;
  text: string;
}

export const readFinalAnswerReceipt = (runDirectory: string, runId: string): FinalAnswerReceipt | undefined => {
  try {
    const value = JSON.parse(readFileRetrying(path.join(runDirectory, "final-answer.json")));
    if (value?.version === 1 && value.runId === runId && typeof value.id === "string" && value.id.length > 0 &&
        Number.isSafeInteger(value.recordedAt) && value.recordedAt > 0 && typeof value.text === "string") return value;
  } catch { /* Missing or invalid receipts never establish a terminal boundary. */ }
  return undefined;
};

/** One native writer owns this run. A retry/late frame can never replace its answer. */
export const saveFinalAnswerReceipt = (runDirectory: string, runId: string, text: string): FinalAnswerReceipt => {
  const existing = readFinalAnswerReceipt(runDirectory, runId);
  if (existing) return existing;
  const file = path.join(runDirectory, "final-answer.json");
  if (fs.existsSync(file)) throw new Error("Invalid or mismatched final-answer receipt already exists");
  const receipt: FinalAnswerReceipt = { version: 1, id: randomUUID(), runId, recordedAt: Date.now(), text };
  writeFileAtomic(file, JSON.stringify(receipt), { durable: true });
  return receipt;
};

/** Text alongside tool calls is progress, not a final answer; truncation/retry isn't success. */
export const finalAssistantText = (message: unknown): string | undefined => {
  if (!message || typeof message !== "object") return undefined;
  const value = message as { role?: unknown; stopReason?: unknown; content?: unknown };
  if (value.role !== "assistant" || value.stopReason !== "stop" || !Array.isArray(value.content) ||
      value.content.some(block => block?.type === "toolCall")) return undefined;
  return value.content.filter(block => block?.type === "text" && typeof block.text === "string")
    .map(block => block.text).join("");
};
