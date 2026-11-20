import { writeFileAtomic } from "../core/atomic-write.js";

/** Persist answer evidence before EOF disposal, never empty unstructured success.
 * Structured control permits empty text; reply/schema validation still owns the
 * result after the execution tree and streams drain.
 */
export const savePiSettlementReceipt = (file: string, runId: string, text: string, structuredReply = false): void => {
  if (!structuredReply && !text.trim()) throw new Error("Cannot persist Pi settlement without final text");
  writeFileAtomic(file, JSON.stringify({ version: 1, runId, outcome: "completed", text }), { durable: true });
};
