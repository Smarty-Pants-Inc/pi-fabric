import type { FabricTurnProvenance } from "../fabric-provenance.js";

/** A final answer fences all later ordinary task input, independently of native close. */
export class FabricTargetTerminalError extends Error {
  override readonly name = "FabricTargetTerminalError";
  readonly code = "FABRIC_TARGET_TERMINAL";
  constructor(readonly targetId: string, readonly finalAnswerReceiptId: string) {
    super(`Fabric task ${targetId} recorded its final answer (${finalAnswerReceiptId}); input cannot revive it. Start a new task.`);
  }
}

export interface AgentTerminalNotice {
  code: "FABRIC_TARGET_TERMINAL";
  targetId: string;
  messageId: string;
  delivery: "steer" | "followUp";
  finalAnswerReceiptId: string;
  sender?: FabricTurnProvenance["sender"];
}

/** Copy the fixed rejection fields through owner ACKs; never trust arbitrary error data. */
export const terminalRejectionFields = (value: unknown): { code: "FABRIC_TARGET_TERMINAL"; finalAnswerReceiptId: string; targetId: string } | undefined => {
  if (!value || typeof value !== "object") return undefined;
  const error = value as Record<string, unknown>;
  if (error.code !== "FABRIC_TARGET_TERMINAL" || typeof error.finalAnswerReceiptId !== "string" || !error.finalAnswerReceiptId ||
      error.finalAnswerReceiptId.length > 200 || typeof error.targetId !== "string" || !error.targetId || error.targetId.length > 200) return undefined;
  return { code: "FABRIC_TARGET_TERMINAL", finalAnswerReceiptId: error.finalAnswerReceiptId, targetId: error.targetId };
};
