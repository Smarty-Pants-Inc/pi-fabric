export const JEV_GUEST_DECLARATIONS = `
type FabricJevJson = null | boolean | number | string | FabricJevJson[] | { [key: string]: FabricJevJson };
type FabricJevDescription = string | FabricJevJson[] | { [key: string]: FabricJevJson };
type FabricJevQuestion =
  | { type: "noul"; instructions: FabricJevDescription; criteria?: { true?: FabricJevDescription; false?: FabricJevDescription } }
  | { type: "choice"; instructions: FabricJevDescription; criteria: Record<string, FabricJevDescription | null> }
  | { type: "score"; instructions: FabricJevDescription; criteria: FabricJevDescription[] };
type FabricJevAnswer<Q extends FabricJevQuestion> = Q extends { type: "noul" }
  ? { type: "noul"; noul: number }
  : Q extends { type: "choice"; criteria: infer C }
  ? { type: "choice"; choice: Extract<keyof C, string>; confidence: number; probabilities: Record<Extract<keyof C, string>, number> }
  : { type: "score"; score: number; confidence: number; probabilities: Record<string, number>; legend: Record<string, FabricJevJson> };
interface FabricJevProgram {
  name: string;
  code: string;
  inputSchema: Record<string, unknown>;
  outputSchema: Record<string, unknown>;
  requires: string[];
  limits?: { timeoutMs?: number; maxEvaluations?: number; maxToolCalls?: number; maxTokens?: number };
}
type FabricJevHostEventName = "input" | "turn_end" | "tool_error" | "agent_end" | "agent_settled";
interface FabricJevObserve {
  events: FabricJevHostEventName[];
  include?: Array<"inputText" | "assistantText" | "toolResults">;
  maxChars?: number; queueSize?: number; maxEventAgeMs?: number;
  delivery?: "steer" | "followUp"; triggerTurn?: boolean; maxAdvice?: number;
}
interface FabricJevHostEvent {
  id: string; sequence: number; event: FabricJevHostEventName; source: "main";
  sessionId: string; revision: number; at: number; payload: FabricJevJson; truncated: boolean;
}
interface FabricJevObservationStats {
  events: FabricJevHostEventName[]; received: number; consumed: number; dropped: number; queued: number;
  adviceDelivered: number; adviceSuppressed: number;
}
interface FabricJevAdviceResult {
  delivered: boolean;
  reason?: "disabled" | "stale" | "duplicate" | "budget" | "feedback" | "delivery_failed";
}
interface FabricJevRun<T = unknown> {
  id: string; name: string; state: "running" | "completed" | "failed" | "cancelled" | "timed_out";
  background: boolean; startedAt: number; endedAt?: number; result?: T; error?: string;
  evaluations: number; toolCalls: number; usage: { input_tokens: number | null; output_tokens: number | null };
  events: Array<{ sequence: number; at: number; value: FabricJevJson }>; nextSequence: number; logs: string[];
  observation?: FabricJevObservationStats;
}
type DecisionJSON = FabricJevJson;
type DecisionDescription = FabricJevDescription;
type DecisionAPI = "system-one" | "cloudflare" | "openai-decisions" | "vercel-evaluate" | "llama-cpp" | "llama-system-one" | "anthropic" | "google";
type DecisionQuestion = Exclude<FabricJevQuestion, { type: "noul" }> | { type: "boolean" | "bool" | "noul" | "predicate"; instructions: DecisionDescription; criteria?: { true?: DecisionDescription; false?: DecisionDescription } };
interface DecisionTarget {
  provider: string; api?: DecisionAPI; model?: string; endpoint?: string;
  allowLocal?: boolean; allowGenerated?: boolean; providerOptions?: Record<string, DecisionJSON>; temperature?: number;
}
interface DecisionProfiles { version: 1; defaultProfile?: string; profiles: Record<string, DecisionTarget> }
interface DecisionRequest extends Partial<DecisionTarget> {
  profile?: string; state: DecisionDescription; questions: Record<string, DecisionQuestion>;
  images?: { type?: "image"; data: string; mimeType: "image/png" | "image/jpeg" | "image/webp" | "image/gif" }[];
}
type DecisionImageSupport = "supported" | "unsupported" | "model-dependent";
interface DecisionResolution {
  target: DecisionTarget & { api: DecisionAPI; model: string; endpoint: string; allowLocal: boolean; allowGenerated: boolean };
  credential: { env: string; resolved: false }; imageSupport: DecisionImageSupport; generated: boolean; profile?: string;
}
interface DecisionProvider { provider: string; api: DecisionAPI | ""; defaultModel: string; endpoint: string; credentialEnv: string; imageSupport: DecisionImageSupport; generated: boolean }
type DecisionAnswer = { type: "boolean"; probability: number }
  | { type: "choice"; choice: string; probabilities?: Record<string, number>; confidence?: number }
  | { type: "score"; score: number; probabilities?: Record<string, number>; confidence?: number; legend?: Record<string, DecisionDescription> }
  | { type: "refusal"; [field: string]: unknown };
interface DecisionResult {
  status: "ok" | "error"; provider: string; api: DecisionAPI; model: string; answers: Record<string, DecisionAnswer>;
  usage: { input_tokens: number | null; output_tokens: number | null };
  provenance?: { kind: "native" | "generated" | "token-logprobs"; [field: string]: unknown };
  raw?: DecisionJSON; rawJson?: string; rawText?: string; rawComplete?: boolean; httpStatus?: number;
  error?: { code: number; message: string } | null; budget: { exhausted: boolean; reason: string };
}
interface DecisionModel {
  model: { provider: string; id: string }; name: string; api: string; target?: DecisionTarget;
  supported: boolean; imageSupport: DecisionImageSupport; generated: boolean; logits: boolean;
  credentials: { configured: boolean; verified: false };
}
interface FabricJevApi {
  decide(request: DecisionRequest): Promise<DecisionResult>;
  resolveDecision(args: { request: DecisionRequest }): Promise<DecisionResolution>;
  decisionProviders(): Promise<DecisionProvider[]>;
  models(): Promise<DecisionModel[]>;
  evaluate<Q extends Record<string, FabricJevQuestion>>(args: {
    state: string | FabricJevJson[] | { [key: string]: FabricJevJson }; questions: Q; model?: string;
  }): Promise<{ model: string; answers: { [K in keyof Q]: FabricJevAnswer<Q[K]> }; usage: { input_tokens: number; output_tokens: number } }>;
  run<T = unknown>(args: { program: FabricJevProgram; input: FabricJevJson }): Promise<FabricJevRun<T>>;
  spawn(args: { program: FabricJevProgram; input: FabricJevJson; observe?: FabricJevObserve }): Promise<FabricJevRun>;
  status(args?: { id?: never; after?: never }): Promise<{ credentials: { configured: boolean; source: "pi" | "environment" | "command" | "missing"; verified: boolean }; model: string; runs: Array<{ id: string; name: string; state: string; background: boolean; startedAt: number; evaluations: number; toolCalls: number; observation?: FabricJevObservationStats }> }>;
  status<T = unknown>(args: { id: string; after?: number }): Promise<FabricJevRun<T>>;
  wait<T = unknown>(args: { id: string }): Promise<FabricJevRun<T>>;
  /** Alias for wait. */
  join<T = unknown>(args: { id: string }): Promise<FabricJevRun<T>>;
  advise(args: { id: string; eventId: string; message: string }): Promise<FabricJevAdviceResult>;
  stop(args: { id: string }): Promise<FabricJevRun>;
}
declare const jev: FabricJevApi;
`;
