import { ModelRoutePinError, resolvePiRoutePin, type PiModelRegistryView } from "../core/model-refresh.js";
import type { FabricModelAliases } from "../core/model-resolution.js";
import { isFabricThinking } from "../thinking.js";
import { decideModelRoute, isRouteAdmissionBlocked, ROUTABLE_CLASSES, type ModelRoutingConfig, type ModelRouteDecision, type RouteEvaluate } from "./model-route.js";

/** Shared auto-task/actor preparation: exact pins, finite authenticated candidates, one shadow Choice. */
export async function prepareModelRoute(input: {
  routeClass: string; protected: unknown; pinModel: unknown; pinThinking: unknown;
  parentSessionId: string; actorId?: string; activationId?: string; modelReason?: string;
  registry: PiModelRegistryView; aliases: FabricModelAliases; config?: ModelRoutingConfig | undefined;
  assertModelAllowed: (model: string) => void; evaluate: RouteEvaluate; signal?: AbortSignal | undefined;
}): Promise<ModelRouteDecision> {
  const { pinModel, pinThinking, registry } = input;
  if (input.config?.live !== undefined && input.config.live !== false) throw new Error("Live model routing is unavailable in shadow mode (#2236)");
  if (typeof pinModel !== "string" || !pinModel.trim() || pinModel === "auto" || !isFabricThinking(pinThinking)) {
    throw new ModelRoutePinError(String(pinModel ?? ""));
  }
  input.assertModelAllowed(pinModel);
  const exact = await resolvePiRoutePin({ selector: pinModel, registry, aliases: input.aliases });
  input.signal?.throwIfAborted();
  const pin = { model: `${exact.provider}/${exact.id}`, effort: pinThinking };
  input.assertModelAllowed(pin.model);
  const available = registry.getAvailable();
  const pinned = available.find(model => `${model.provider}/${model.id}` === pin.model);
  if (pinned && typeof (pinned as { reasoning?: unknown }).reasoning === "boolean") {
    const { getSupportedThinkingLevels } = await import("@earendil-works/pi-ai");
    if (!getSupportedThinkingLevels(pinned as Parameters<typeof getSupportedThinkingLevels>[0]).includes(pinThinking)) {
      throw Object.assign(new Error(`MODEL_ROUTE_EFFORT_UNSUPPORTED: ${pin.model} cannot honor required effort ${pinThinking}; task was not sent`), {
        name: "ModelRouteEffortPinError", code: "MODEL_ROUTE_EFFORT_UNSUPPORTED",
      });
    }
  }
  const candidates = input.config?.shadowCandidates ?? [];
  let candidatesValid = candidates.length <= 16 && candidates.every(candidate =>
    isFabricThinking(candidate.effort) && available.some(model => `${model.provider}/${model.id}` === candidate.model));
  // Live execution must honor the same deny policy and supported effort floor as its pin.
  if (candidatesValid) {
    try {
      for (const candidate of candidates) {
        input.assertModelAllowed(candidate.model);
        const model = available.find(model => `${model.provider}/${model.id}` === candidate.model)!;
        if (typeof (model as { reasoning?: unknown }).reasoning === "boolean") {
          const { getSupportedThinkingLevels } = await import("@earendil-works/pi-ai");
          if (!getSupportedThinkingLevels(model as Parameters<typeof getSupportedThinkingLevels>[0]).includes(candidate.effort)) candidatesValid = false;
        }
      }
    } catch { candidatesValid = false; }
  }
  const reset = input.config?.revertReset?.[input.routeClass] ?? "";
  let live = input.protected === false && ROUTABLE_CLASSES.includes(input.routeClass) && input.config?.liveClasses?.includes(input.routeClass) === true;
  let admissionReason: "admission-blocked" | "admission-state-error" | undefined;
  if (live) {
    try { if (isRouteAdmissionBlocked(input.routeClass, reset)) { live = false; admissionReason = "admission-blocked"; } }
    catch { live = false; admissionReason = "admission-state-error"; }
  }
  const decision = await decideModelRoute({ routeClass: input.routeClass, protected: input.protected, pin, candidates,
    candidatesValid, live, revertReset: reset, parentSessionId: input.parentSessionId,
    ...(input.modelReason !== undefined ? { modelReason: input.modelReason } : {}),
    ...(input.actorId ? { actorId: input.actorId } : {}), ...(input.activationId ? { activationId: input.activationId } : {}) }, input.evaluate, input.signal);
  if (admissionReason) { decision.mode = "shadow"; decision.reasonCode = admissionReason; Object.assign(decision, pin); }
  return decision;
}
