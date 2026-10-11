// Mirrors the standalone lossless decision protocol; never convert through JevResponse.
export type DecisionJSON = null | boolean | number | string | DecisionJSON[] | { [key: string]: DecisionJSON };
export type DecisionDescription = string | DecisionJSON[] | { [key: string]: DecisionJSON };
export type DecisionQuestion =
  | { type: "choice"; instructions: DecisionDescription; criteria: Record<string, DecisionDescription | null> }
  | { type: "score"; instructions: DecisionDescription; criteria: DecisionDescription[] }
  | { type: "boolean" | "bool" | "noul" | "predicate"; instructions: DecisionDescription; criteria?: { true?: DecisionDescription; false?: DecisionDescription } };
export type DecisionAPI = "system-one" | "cloudflare" | "openai-decisions" | "vercel-evaluate" | "llama-cpp" | "llama-system-one" | "anthropic" | "google";
export interface DecisionTarget {
  provider: string;
  api?: DecisionAPI;
  model?: string;
  endpoint?: string;
  allowLocal?: boolean;
  allowGenerated?: boolean;
  providerOptions?: Record<string, DecisionJSON>;
  temperature?: number;
}
export interface DecisionProfiles {
  version: 1;
  defaultProfile?: string;
  profiles: Record<string, DecisionTarget>;
}
export interface DecisionRequest extends Partial<DecisionTarget> {
  profile?: string;
  state: DecisionDescription;
  questions: Record<string, DecisionQuestion>;
  images?: { type?: "image"; data: string; mimeType: "image/png" | "image/jpeg" | "image/webp" | "image/gif" }[];
}
export type DecisionImageSupport = "supported" | "unsupported" | "model-dependent";
export interface DecisionProvider {
  provider: string;
  api: DecisionAPI | "";
  defaultModel: string;
  endpoint: string;
  credentialEnv: string;
  imageSupport: DecisionImageSupport;
  generated: boolean;
}
export interface DecisionResolution {
  target: DecisionTarget & { api: DecisionAPI; model: string; endpoint: string; allowLocal: boolean; allowGenerated: boolean };
  credential: { env: string; resolved: false };
  imageSupport: DecisionImageSupport;
  generated: boolean;
  profile?: string;
}
export type DecisionAnswer =
  | { type: "boolean"; probability: number }
  | { type: "choice"; choice: string; probabilities?: Record<string, number>; confidence?: number }
  | { type: "score"; score: number; probabilities?: Record<string, number>; confidence?: number; legend?: Record<string, DecisionDescription> }
  | { type: "refusal"; [field: string]: unknown };
export interface DecisionResult {
  status: "ok" | "error";
  provider: string;
  api: DecisionAPI;
  model: string;
  answers: Record<string, DecisionAnswer>;
  usage: { input_tokens: number | null; output_tokens: number | null };
  provenance?: { kind: "native" | "generated" | "token-logprobs"; [field: string]: unknown };
  raw?: DecisionJSON;
  rawJson?: string;
  rawText?: string;
  rawComplete?: boolean;
  httpStatus?: number;
  error?: { code: number; message: string } | null;
  budget: { exhausted: boolean; reason: string };
}
export interface DecisionModel {
  /** Pi registry handle; not a native preset or a verified live model. */
  model: { provider: string; id: string };
  name: string;
  api: string;
  target?: DecisionTarget;
  supported: boolean;
  imageSupport: DecisionImageSupport;
  generated: boolean;
  logits: boolean;
  credentials: { configured: boolean; verified: false };
}
