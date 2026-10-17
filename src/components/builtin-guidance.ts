import type { FabricConfig } from "../config.js";
import type { FabricOwnedModelGuidance } from "./model-guidance.js";

// Built-in execution/provider advice belongs in on-demand documentation, not
// a system-prompt digest. Retain the public hook for callers and components.
export const builtinModelGuidance = (
  config: Pick<FabricConfig, "jev" | "schema">,
): FabricOwnedModelGuidance[] => [];
