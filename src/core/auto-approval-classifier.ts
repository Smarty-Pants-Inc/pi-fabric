import type { Usage } from "@earendil-works/pi-ai/compat";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { DEFAULT_JEV_CONFIG, type FabricJevConfig } from "../jev/config.js";
import type { ResolvedFabricAction } from "./action-registry.js";

// Policies and evidence projection load only when an action needs classification,
// never during classifier construction or extension registration.

/** Jev path only: the typed probabilities that produced the decision. */
export interface FabricAutoApprovalVerdicts {
  safeToAutoApprove: number;
  touchesSecrets: number;
  destructive: number;
  targetsAgentArtifacts: number;
}

export interface FabricAutoApprovalDecision {
  decision: "allow" | "escalate";
  reason: string;
  model: string;
  usage: Usage;
  /** Jev path only: per-question probabilities behind the decision. */
  verdicts?: FabricAutoApprovalVerdicts;
  /** Jev path only: effective auto-approval threshold. */
  threshold?: number;
}

export class FabricAutoApprovalClassifier {
  constructor(readonly getJevConfig: () => FabricJevConfig = () => DEFAULT_JEV_CONFIG) {}

  async classify(
    action: ResolvedFabricAction,
    args: Record<string, unknown>,
    context: ExtensionContext,
    modelKey?: string,
  ): Promise<FabricAutoApprovalDecision> {
    context.signal?.throwIfAborted();
    const { classifyAutoApproval } = await import("./auto-approval-classifier-impl.js");
    context.signal?.throwIfAborted();
    return classifyAutoApproval(this, action, args, context, modelKey);
  }
}
