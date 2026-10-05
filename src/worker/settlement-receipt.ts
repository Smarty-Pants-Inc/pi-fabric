import { writeFileAtomic } from "../core/atomic-write.js";

/** Native success can be intentionally empty (or tool-only). Persist its receipt
 * before EOF asks extensions to dispose; process exit is a separate obligation.
 * Missing/invalid structured replies are still validated after the owned tree drains.
 */
export const savePiSettlementReceipt = (file: string, runId: string, text: string): void => {
  writeFileAtomic(file, JSON.stringify({ version: 1, runId, outcome: "completed", text }), { durable: true });
};
