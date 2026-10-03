import fs from "node:fs";
import path from "node:path";

export const DEFAULT_FOLLOW_UP_DEADLINE_MS = 10 * 60_000;
export type FollowUpDeliveryState = "queued" | "settling" | "delivered" | "cancelled";

export function followUpFile(runDirectory: string, messageId: string): string {
  if (!/^[0-9a-f-]{36}$/.test(messageId)) throw new Error(`Unknown follow-up: ${messageId}`);
  return path.join(runDirectory, "follow-ups", messageId + ".json");
}

export function followUpState(file: string): FollowUpDeliveryState {
  const settled = file + ".settled";
  if (!fs.existsSync(settled)) return "queued";
  try {
    const state = fs.readFileSync(path.join(settled, "state"), "utf8");
    if (state === "delivered" || state === "cancelled") return state;
  } catch { /* An exclusive transition is still in progress; never deliver again. */ }
  return "settling";
}

/** Parent cancellation and native boundary delivery compete for one filesystem claim.
 * A crashed claim stays fenced: uncertainty must not turn into a duplicate delivery.
 */
export function settleFollowUp(file: string, state: "delivered" | "cancelled", deliver?: () => void): FollowUpDeliveryState {
  const settled = file + ".settled";
  try { fs.mkdirSync(settled, { mode: 0o700 }); } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST") return followUpState(file);
    throw error;
  }
  // Do not release the claim if delivery throws: it may already have side effects.
  deliver?.();
  fs.writeFileSync(path.join(settled, "state"), state, { mode: 0o600 });
  return state;
}
