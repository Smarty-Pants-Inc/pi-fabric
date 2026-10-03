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

/** The delivery envelope is disposable only after a final receipt. Admissions
 * and receipts remain as the cancellation/replay fence for this run. */
export function releaseFollowUpPayload(file: string): void {
  const state = followUpState(file);
  if (state !== "delivered" && state !== "cancelled") return;
  const payload = path.join(path.dirname(path.dirname(file)), "deliveries", path.basename(file));
  try { fs.unlinkSync(payload); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
}
/** A per-message transport identity, stripped before provider context consumption.
 * Text equality is not an identity: two identical follow-ups must settle separately. */
export const followUpMessage = (id: string, text: string): string => `[fabric-follow-up:${id}]\n${text}`;
export const followUpMessageId = (text: string): string | undefined =>
  /^\[fabric-follow-up:([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\]\n/.exec(text)?.[1];
/** Parent cancellation and native boundary delivery compete for one filesystem claim.
 * Native submission must not claim: only receiver context consumption may settle delivery.
 * A crashed claim stays fenced: uncertainty must not turn into a duplicate delivery.
 */
export function settleFollowUp(file: string, state: "delivered" | "cancelled"): FollowUpDeliveryState {
  const settled = file + ".settled";
  try { fs.mkdirSync(settled, { mode: 0o700 }); } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST") return followUpState(file);
    throw error;
  }
  // A crashed write stays uncertain and retains the worker-owned payload.
  fs.writeFileSync(path.join(settled, "state"), state, { mode: 0o600 });
  return state;
}
