import type {
  ExtensionAPI,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import type { CompactRequestIntent } from "../core/compact-controller.js";
import { setModelSafely } from "./model-switch.js";
import { PREWALK_CONTINUE_MESSAGE_TYPE } from "./messages.js";
import type {
  FabricPrewalkBorrowedMain,
  PrewalkController,
} from "./controller.js";

// The return half of in-place Prewalk: restoring the borrowed Main model after a
// continuation, restart-safe recovery from the persisted branch, and the
// settle-on-return path. Session start and /fabric reload run this eagerly, so it
// stays free of the boundary engine's executor dependencies.
const modelForReturnKey = (key: string, context: ExtensionContext) => {
  const separator = key.indexOf("/");
  if (separator <= 0 || separator === key.length - 1) return undefined;
  return context.modelRegistry.find(key.slice(0, separator), key.slice(separator + 1));
};

const PREWALK_RETURN_COMPACTION_INSTRUCTIONS = [
  "Compact before Main returns to its boundary model after an in-place prewalk continuation.",
  "Preserve the executor's final report and verification results; summarize implementation scratch work, file reads, and command output.",
].join(" ");

export interface InPlacePrewalkSettleOptions {
  // Enabled by default when a compact controller is provided.
  compactOnReturn?: boolean;
  compact?: {
    request(intent: CompactRequestIntent): unknown;
    maybeCommit(context: ExtensionContext): Promise<void>;
    status?(): { pending?: unknown };
  };
}
// A restarted process has no in-memory borrow record, yet the branch still
// proves Main is owed a return: a persisted in-place continuation whose
// executor switch is the session's last recorded model change. A later model
// change (completed return, manual pick, another cycle) supersedes it, and a
// malformed or trajectory record carries no return identity.
const prewalkRecoveryCandidate = (
  branch: ReadonlyArray<unknown>,
): FabricPrewalkBorrowedMain | undefined => {
  let continuationIndex = -1;
  let details: { continuationId?: unknown; model?: unknown; returnModel?: unknown } | undefined;
  for (let index = branch.length - 1; index >= 0; index -= 1) {
    const entry = branch[index] as { type?: unknown; customType?: unknown; details?: unknown };
    if (entry?.type !== "custom_message" || entry.customType !== PREWALK_CONTINUE_MESSAGE_TYPE) {
      continue;
    }
    if (typeof entry.details !== "object" || entry.details === null) continue;
    if ((entry.details as { mode?: unknown }).mode !== "in-place") continue;
    continuationIndex = index;
    details = entry.details as { continuationId?: unknown; model?: unknown; returnModel?: unknown };
    break;
  }
  if (continuationIndex < 0 || !details) return undefined;
  const continuationId = typeof details.continuationId === "string" ? details.continuationId : "";
  const executorModel = typeof details.model === "string" ? details.model.trim() : "";
  const returnModel = typeof details.returnModel === "string" ? details.returnModel.trim() : "";
  if (!continuationId || !executorModel || !returnModel || returnModel === executorModel) {
    return undefined;
  }
  let recordedExecutor: string | undefined;
  for (let index = 0; index < branch.length; index += 1) {
    const entry = branch[index] as { type?: unknown; provider?: unknown; modelId?: unknown };
    if (entry?.type !== "model_change") continue;
    if (index > continuationIndex) return undefined;
    recordedExecutor = `${String(entry.provider)}/${String(entry.modelId)}`;
  }
  return recordedExecutor === executorModel ? { returnModel, executorModel } : undefined;
};
export const restoreBorrowedInPlaceMain = async (
  controller: PrewalkController,
  extension: ExtensionAPI,
  context: ExtensionContext,
): Promise<boolean> => {
  let borrowed = controller?.borrowedReturn?.();
  let recovered = false;
  if (!borrowed) {
    // Restart recovery: adopt the persisted continuation's return tuple, then
    // restore through the same path. Success clears the adopted record — the
    // restore's recorded model change supersedes the historical candidate —
    // while failure keeps it so the auto-arm guard can still refuse to capture
    // the executor as Main.
    const candidate = prewalkRecoveryCandidate(context.sessionManager?.getBranch?.() ?? []);
    if (!candidate || !controller.hydrateBorrowedMain(candidate)) return false;
    borrowed = candidate;
    recovered = true;
  }
  const currentKey = context.model
    ? `${context.model.provider}/${context.model.id}`
    : undefined;
  // Only snap back when this session is still on the executor we switched to.
  // A new session that already loaded Main, or a later manual pick, stays put.
  if (currentKey !== undefined && currentKey !== borrowed.executorModel) return false;
  if (currentKey === borrowed.returnModel) return false;

  const model = modelForReturnKey(borrowed.returnModel, context);
  if (!model) {
    context.ui.setStatus("fabric-prewalk", `return failed → ${borrowed.returnModel}`);
    context.ui.notify(
      `Prewalk left Main on the executor; could not restore unavailable model ${borrowed.returnModel}.`,
      "error",
    );
    return false;
  }

  const restored = await setModelSafely(extension, model);
  if (!restored) {
    context.ui.setStatus("fabric-prewalk", `return failed → ${borrowed.returnModel}`);
    context.ui.notify(
      `Prewalk left Main on the executor; could not return to ${borrowed.returnModel}. Check model authentication.`,
      "error",
    );
    return false;
  }

  if (recovered) controller.clearBorrowed();
  context.ui.notify(`Restored Main to ${borrowed.returnModel} after in-place prewalk.`, "info");
  return true;
};
export const settleInPlacePrewalk = async (
  controller: PrewalkController,
  extension: ExtensionAPI,
  context: ExtensionContext,
  options?: InPlacePrewalkSettleOptions,
): Promise<boolean> => {
  const sessionId = context.sessionManager.getSessionId();
  const settlement = controller.takeContinuationSettlement(sessionId);
  if (!settlement) return false;

  const model = modelForReturnKey(settlement.returnModel, context);
  if (!model) {
    // A failed return is not completion: dropping to the shared cancel path
    // disarms without re-arming while preserving borrowedReturn, so a later
    // session start or /fabric reload can still restore Main. Repeated settle
    // calls find no continuation and stay quiet.
    controller.cancel();
    context.ui.setStatus("fabric-prewalk", `return failed → ${settlement.returnModel}`);
    context.ui.notify(
      `Prewalk completed, but Main could not return to unavailable model ${settlement.returnModel}.`,
      "error",
    );
    return false;
  }

  context.ui.setStatus("fabric-prewalk", `returning Main → ${settlement.returnModel}`);
  if (options?.compact && options.compactOnReturn !== false) {
    // Compact while the executor is still active so the restored boundary
    // model re-ingests a compacted transcript instead of the executor's full
    // implementation scratch work: the return prefill is cold regardless of
    // provider cache-policy differences, so keep it small. An already-pending
    // intent (e.g. requested by the model) wins over ours. The commit is
    // best-effort; the controller records failures without throwing.
    if (!options.compact.status?.().pending) {
      options.compact.request({
        reason: "in-place prewalk return",
        instructions: PREWALK_RETURN_COMPACTION_INSTRUCTIONS,
        requestedBy: "prewalk",
      });
    }
    await options.compact.maybeCommit(context);
  }
  const restored = await setModelSafely(extension, model);
  if (!restored) {
    // A failed return is not completion: dropping to the shared cancel path
    // disarms without re-arming while preserving borrowedReturn, so a later
    // session start or /fabric reload can still restore Main. Repeated settle
    // calls find no continuation and stay quiet.
    controller.cancel();
    context.ui.setStatus("fabric-prewalk", `return failed → ${settlement.returnModel}`);
    context.ui.notify(
      `Prewalk completed, but Main could not return to ${settlement.returnModel}. Check model authentication.`,
      "error",
    );
    return false;
  }

  controller.finishContinuation(sessionId, settlement.continuationId);
  const status = controller.status();
  context.ui.setStatus(
    "fabric-prewalk",
    status.state === "armed" ? `armed → ${status.model}` : undefined,
  );
  context.ui.notify(
    status.state === "armed"
      ? `Prewalk complete. Main returned to ${settlement.returnModel} and re-armed for the next task.`
      : `Prewalk complete. Main returned to ${settlement.returnModel}.`,
    "info",
  );
  return true;
};
