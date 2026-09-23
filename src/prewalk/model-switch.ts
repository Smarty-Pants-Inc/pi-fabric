import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

// The single guarded model-switch helper, shared by the boundary engine's
// rollback path and the return path. Small, dependency-free and eager-safe.
// A model switch can reject or throw, and every return site needs the same
// answer; the caller reports the failure, so the cause is not rethrown here.
export const setModelSafely = async (
  extension: ExtensionAPI,
  model: Parameters<ExtensionAPI["setModel"]>[0],
): Promise<boolean> => {
  try {
    return await extension.setModel(model);
  } catch {
    return false;
  }
};
