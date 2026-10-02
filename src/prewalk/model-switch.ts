import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { assertFabricModelAllowed, FabricModelDeniedError, type FabricModelPolicy } from "../core/model-policy.js";

// A getter keeps long-running compaction/recovery tied to current host policy.
export type PrewalkModelPolicy = FabricModelPolicy | (() => FabricModelPolicy);
export const assertPrewalkModelAllowed = (key: string, policy?: PrewalkModelPolicy): void => {
  assertFabricModelAllowed(key, typeof policy === "function" ? policy() : policy);
};

// Policy refusal is typed and must escape; only authentication/switch failures
// retain the existing best-effort boolean contract.
export const setModelSafely = async (
  extension: ExtensionAPI,
  model: Parameters<ExtensionAPI["setModel"]>[0],
  policy?: PrewalkModelPolicy,
  requestedKey = `${model.provider}/${model.id}`,
): Promise<boolean> => {
  assertPrewalkModelAllowed(requestedKey, policy);
  assertPrewalkModelAllowed(`${model.provider}/${model.id}`, policy);
  try {
    return await extension.setModel(model);
  } catch (error) {
    if (error instanceof FabricModelDeniedError) throw error;
    return false;
  }
};
