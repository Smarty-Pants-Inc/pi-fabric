import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { CompactionHookOptions } from "./hook.js";

// Registration and idle lifecycle need neither the cut planner nor summary projections.
// hook.js is a stable build entry, so delayed loads survive installed-generation swaps.
export const registerLazyCompactionHook = (pi: ExtensionAPI, options: CompactionHookOptions): void => {
  pi.on("session_before_compact", async (event, context) => {
    if (event.customInstructions === "__pi_vcc__") return;
    const { handleFabricBeforeCompact } = await import("./hook.js");
    return handleFabricBeforeCompact(event, context, options, pi);
  });
  pi.on("session_before_tree", async (event, context) => {
    if (options.getEngine() !== "fabric" || !event.preparation.userWantsSummary
      || event.preparation.replaceInstructions === true) return;
    const { handleFabricBeforeTree } = await import("./hook.js");
    return handleFabricBeforeTree(event, context, options);
  });
};
