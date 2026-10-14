import type { FabricTurnProvenance } from "../fabric-provenance.js";

/** A final answer fences all later ordinary task input, independently of native close. */
export class FabricTargetTerminalError extends Error {
  override readonly name = "FabricTargetTerminalError";
  readonly code = "FABRIC_TARGET_TERMINAL";
  readonly reason = "run-ended";
  readonly deliveryStatus = "undelivered";
  constructor(readonly targetId: string, readonly finalAnswerReceiptId: string,
    readonly runState: FabricTargetRunState = "final-answer-recorded") {
    super(`undelivered: task already finished — Fabric task ${targetId} run ended (${runState}); it recorded its final answer (${finalAnswerReceiptId}); input cannot revive it. Start a new task.`);
  }
}

export type FabricTargetRunState = "final-answer-pending" | "final-answer-recorded" | "completed" | "failed" | "stopped" | "timed_out";

/** One admitted control lost the final-answer race; it was not consumed. */
export interface AgentUndeliveredMessage {
  code: "FABRIC_TARGET_TERMINAL";
  reason: "run-ended";
  deliveryStatus: "undelivered";
  messageId: string;
  delivery: "steer" | "followUp";
  finalAnswerReceiptId: string;
  /** The target's real run state when the append lost the race; carried to remote errors. */
  runState: FabricTargetRunState;
  sender?: FabricTurnProvenance["sender"];
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
export const terminalRejectionFields = (value: unknown): { code: "FABRIC_TARGET_TERMINAL"; finalAnswerReceiptId: string; targetId: string; reason?: "run-ended"; deliveryStatus?: "undelivered"; runState?: FabricTargetRunState } | undefined => {
  if (!value || typeof value !== "object") return undefined;
  const error = value as Record<string, unknown>;
  if (error.code !== "FABRIC_TARGET_TERMINAL" || typeof error.finalAnswerReceiptId !== "string" || !error.finalAnswerReceiptId ||
      error.finalAnswerReceiptId.length > 200 || typeof error.targetId !== "string" || !error.targetId || error.targetId.length > 200) return undefined;
  const runState = typeof error.runState === "string" && ["final-answer-pending", "final-answer-recorded", "completed", "failed", "stopped", "timed_out"].includes(error.runState)
    ? error.runState as FabricTargetRunState : undefined;
  return { code: "FABRIC_TARGET_TERMINAL", finalAnswerReceiptId: error.finalAnswerReceiptId, targetId: error.targetId,
    ...(error.reason === "run-ended" ? { reason: "run-ended" as const } : {}),
    ...(error.deliveryStatus === "undelivered" ? { deliveryStatus: "undelivered" as const } : {}), ...(runState ? { runState } : {}) };
};
