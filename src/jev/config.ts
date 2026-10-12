import { isJevModelId } from "./routes.js";
import { normalizeDecisionProfiles } from "./decision-profiles.js";
import type { DecisionProfiles } from "./decision-types.js";

export interface FabricJevConfig {
  enabled: boolean;
  /** Trusted routing-only profiles; independent of legacy evaluate and auto approval. */
  decisionProfiles?: DecisionProfiles;
  /** Optional trusted selector, overriding the document default; never affects auto approval. */
  decisionProfile?: string | null;
  model: string;
  /** Minimum Noul safety probability for Jev auto approvals (0–1). */
  autoApprovalThreshold: number;
  /** Trusted host configuration only; argv, never a shell expression. */
  credentialCommand: string[];
  requestTimeoutMs: number;
  maxRequestBytes: number;
  maxConcurrentRuns: number;
  maxRetainedRuns: number;
  maxDurationMs: number;
  maxEvaluations: number;
  maxToolCalls: number;
  maxTokens: number;
  /**
   * Where Jev program decisions are dispatched. `fabric` (default) is always
   * in-process. `auto` opts into one jev-fabric `serve` connection per program
   * run when a compatible binary is available (macOS/Linux), else in-process;
   * `jev-fabric` requires it. Direct calls and auto approvals stay in-process.
   */
  transport: "auto" | "fabric" | "jev-fabric";
}
export const DEFAULT_JEV_CONFIG: FabricJevConfig = {
  enabled: true,
  model: "jev-latest",
  autoApprovalThreshold: 0.5,
  credentialCommand: [],
  requestTimeoutMs: 15_000,
  maxRequestBytes: 131_072,
  maxConcurrentRuns: 4,
  maxRetainedRuns: 64,
  maxDurationMs: 900_000,
  maxEvaluations: 1_000,
  maxToolCalls: 10_000,
  maxTokens: 1_000_000,
  transport: "fabric",
};
export function normalizeJevConfig(value: unknown): FabricJevConfig {
  const input = value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown> : {};
  const integer = (key: keyof FabricJevConfig, max: number, min = 1): number => {
    const v = input[key];
    return typeof v === "number" && Number.isFinite(v)
      ? Math.max(min, Math.min(max, Math.floor(v))) : DEFAULT_JEV_CONFIG[key] as number;
  };
  const command = input.credentialCommand;
  const decisionProfiles = input.decisionProfiles === undefined ? undefined : normalizeDecisionProfiles(input.decisionProfiles);
  const decisionProfile = input.decisionProfile;
  if (decisionProfile !== undefined && decisionProfile !== null && (typeof decisionProfile !== "string" || !/^[A-Za-z0-9_.-]{1,128}$/.test(decisionProfile) || !decisionProfiles || !Object.hasOwn(decisionProfiles.profiles, decisionProfile)))
    throw new Error("jev.decisionProfile must name a known decision profile");
  return {
    enabled: typeof input.enabled === "boolean" ? input.enabled : true,
    ...(decisionProfiles ? { decisionProfiles } : {}),
    ...(decisionProfile === null || typeof decisionProfile === "string" ? { decisionProfile } : {}),
    // Bare aliases use the direct route; `~typesafe/...` and `typesafe/...` select OpenRouter decisions.
    model: typeof input.model === "string" && isJevModelId(input.model)
      ? input.model : DEFAULT_JEV_CONFIG.model,
    autoApprovalThreshold: typeof input.autoApprovalThreshold === "number" &&
      Number.isFinite(input.autoApprovalThreshold) && input.autoApprovalThreshold >= 0 && input.autoApprovalThreshold <= 1
      ? input.autoApprovalThreshold : DEFAULT_JEV_CONFIG.autoApprovalThreshold,
    credentialCommand: Array.isArray(command) && command.length <= 16 &&
      command.every(v => typeof v === "string" && v.length > 0 && v.length <= 4096 && !v.includes("\0"))
      ? [...command] as string[] : [],
    requestTimeoutMs: integer("requestTimeoutMs", 120_000, 100),
    maxRequestBytes: integer("maxRequestBytes", 1_048_576, 1024),
    maxConcurrentRuns: integer("maxConcurrentRuns", 16),
    maxRetainedRuns: integer("maxRetainedRuns", 256),
    maxDurationMs: integer("maxDurationMs", 86_400_000, 100),
    maxEvaluations: integer("maxEvaluations", 100_000),
    maxToolCalls: integer("maxToolCalls", 1_000_000),
    maxTokens: integer("maxTokens", 100_000_000),
    transport: input.transport === "auto" || input.transport === "jev-fabric" ? input.transport : "fabric",
  };
}
