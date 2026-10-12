export type JevJson = null | boolean | number | string | JevJson[] | { [key: string]: JevJson };
export type JevDescription = string | JevJson[] | { [key: string]: JevJson };
export type JevQuestion =
  | { type: "noul"; instructions: JevDescription; criteria?: { true?: JevDescription; false?: JevDescription } }
  | { type: "choice"; instructions: JevDescription; criteria: Record<string, JevDescription | null> }
  | { type: "score"; instructions: JevDescription; criteria: JevDescription[] };
export interface JevRequest {
  state: string | JevJson[] | { [key: string]: JevJson };
  questions: Record<string, JevQuestion>;
  model?: string;
}
export type JevAnswer =
  | { type: "noul"; noul: number }
  | { type: "choice"; choice: string; confidence: number; probabilities: Record<string, number> }
  | { type: "score"; score: number; confidence: number; probabilities: Record<string, number>; legend: Record<string, JevJson> };
export interface JevResponse {
  model: string;
  answers: Record<string, JevAnswer>;
  usage: { input_tokens: number; output_tokens: number };
}
export interface JevProgram {
  name: string;
  code: string;
  inputSchema: Record<string, unknown>;
  outputSchema: Record<string, unknown>;
  requires: string[];
  limits?: { timeoutMs?: number; maxEvaluations?: number; maxToolCalls?: number; maxTokens?: number };
}
export type JevHostEventName = "input" | "turn_end" | "tool_error" | "agent_end" | "agent_settled";
export interface JevObserve {
  events: JevHostEventName[];
  /** No content is shared unless explicitly selected. Never includes thinking, images, tool arguments, or history. */
  include?: Array<"inputText" | "assistantText" | "toolResults">;
  maxChars?: number;
  queueSize?: number;
  maxEventAgeMs?: number;
  /** Omit to record judgments without sending messages to Main. */
  delivery?: "steer" | "followUp";
  triggerTurn?: boolean;
  maxAdvice?: number;
}
export interface JevHostEvent {
  id: string;
  sequence: number;
  event: JevHostEventName;
  source: "main";
  sessionId: string;
  revision: number;
  at: number;
  payload: JevJson;
  truncated: boolean;
}
export interface JevObservationStats {
  events: JevHostEventName[];
  received: number;
  consumed: number;
  dropped: number;
  queued: number;
  adviceDelivered: number;
  adviceSuppressed: number;
}
export interface JevAdviceResult {
  delivered: boolean;
  reason?: "disabled" | "stale" | "duplicate" | "budget" | "feedback" | "delivery_failed";
}
export interface JevAdvice {
  runId: string;
  name: string;
  eventId: string;
  message: string;
  delivery: "steer" | "followUp";
  triggerTurn: boolean;
}
export interface JevLaunch { program: JevProgram; input: JevJson; observe?: JevObserve }
export type JevRunState = "running" | "completed" | "failed" | "cancelled" | "timed_out";
export interface JevEvent { sequence: number; at: number; value: JevJson }
export interface JevRunInfo {
  id: string;
  name: string;
  state: JevRunState;
  background: boolean;
  startedAt: number;
  endedAt?: number;
  result?: JevJson;
  error?: string;
  evaluations: number;
  toolCalls: number;
  /** Null means unknown billed usage, never zero. Further inference is blocked. */
  usage: { input_tokens: number | null; output_tokens: number | null };
  events: JevEvent[];
  nextSequence: number;
  logs: string[];
  observation?: JevObservationStats;
}
