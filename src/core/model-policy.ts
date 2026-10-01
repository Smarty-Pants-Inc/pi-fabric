/** Trusted host policy for every Fabric participant model selection. */
export interface FabricModelPolicy {
  deniedModels?: readonly string[];
  deniedModelReplacement?: string;
}

/** Refusal, not a retryable availability miss or an automatic model-family switch. */
export class FabricModelDeniedError extends Error {
  readonly code = "FABRIC_MODEL_DENIED";
  constructor(readonly model: string, readonly replacement?: string) {
    super(`Fabric model ${JSON.stringify(model)} is denied by fleet policy #2236. ` +
      (replacement ? `Use ${replacement} instead.` : "Ask the host administrator for an allowed replacement."));
    this.name = "FabricModelDeniedError";
  }
}

/** Check both raw intent and the canonical resolved key, case-insensitively. */
export const assertFabricModelAllowed = (model: string | undefined, policy?: FabricModelPolicy): void => {
  const key = model?.trim().toLowerCase();
  if (key && policy?.deniedModels?.some((denied) => denied.trim().toLowerCase() === key)) {
    throw new FabricModelDeniedError(key, policy.deniedModelReplacement?.trim() || undefined);
  }
};
