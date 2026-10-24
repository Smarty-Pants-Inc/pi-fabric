import type { AgentRunRequest } from "./types.js";
import type { FabricAgentRunner } from "../config.js";
import type { FabricThinking } from "../thinking.js";
import type { FabricPlacementDeciderConfig } from "./placement-decider-config.js";

export type PlacementDiagnostic = (kind: string, id: string, missingFields?: string[]) => void;

/** Optional admission orchestration stays outside the eager AgentManager startup graph. */
export async function runPlacementPreflight(
  config: FabricPlacementDeciderConfig,
  request: AgentRunRequest,
  runner: FabricAgentRunner,
  model: string | undefined,
  thinking: FabricThinking | undefined,
  id: string,
  mainAgentId: string | undefined,
  diagnostic: PlacementDiagnostic,
): Promise<void> {
  if (config.mode === "off" || (runner !== "pi" && runner !== "pi-durable")) return;
  const [{ CliPlacementDecider, PlacementAdmissionError, PlacementQueueError }, contextModule, contract] = await Promise.all([
    import("./placement-admission.js"), import("./placement-context.js"), import("./placement-contract.js"),
  ]);
  const deadline = performance.now() + contract.PLACEMENT_DEADLINE_MS;
  const binding = request.placement;
  const actorActivation = binding?.actorActivation;
  if ((actorActivation && (binding?.role !== "actor-activation" || actorActivation.actorId !== request.actorId ||
      actorActivation.rootId !== mainAgentId)) ||
      (binding?.role === "actor-activation" && (!actorActivation || !request.actorId || !mainAgentId))) {
    diagnostic("actor-root-ownership-refused", id);
    throw new PlacementAdmissionError("invalid-request");
  }
  const launchId = binding?.launchId ?? request.activationId ?? id;
  const generation = binding?.generation ?? 1;
  if (!Number.isSafeInteger(generation) || generation < 1) throw new PlacementAdmissionError("invalid-request");
  const context = await contextModule.resolvePlacementCallerContext(config, deadline);
  const draft = contextModule.buildPlacementRequestDraft(context, {
    launchId, generation, agentName: `fabric-child:${launchId}`,
    ...(binding ? { child: {
      ...(binding.role ? { role: binding.role } : {}), ...(binding.complexity ? { complexity: binding.complexity } : {}),
      ...(binding.profile ? { profile: binding.profile } : {}), ...(binding.memoryProfileRef ? { memoryProfileRef: binding.memoryProfileRef } : {}),
      ...(binding.actorActivation ? { actorActivation: binding.actorActivation } : {}),
    } } : {}),
    ...(model ? { model } : {}), ...(thinking ? { thinking } : {}),
    capabilities: request.capabilityRequirements ?? [],
  });
  const decider = new CliPlacementDecider(config);
  if (config.mode === "shadow") {
    const observation = await decider.observe(draft.request, { deadline });
    diagnostic(`shadow-${observation.hypothetical.decision.toLowerCase()}`, id, draft.missingFields);
    return;
  }
  if (draft.missingFields.length || !draft.roleAllowed) {
    diagnostic("context-or-role-refused", id, draft.missingFields);
    throw new PlacementAdmissionError("invalid-request");
  }
  const answer = await decider.decide(draft.request, { deadline });
  if (answer.decision === "QUEUE") {
    if (draft.request.interaction.class === "live-human") {
      diagnostic("live-human-queue-refused", id);
      throw new PlacementAdmissionError("invalid-response");
    }
    const anonymousUnavailable = answer.reason === "DECIDER_UNAVAILABLE" && answer.launch_id === undefined && answer.generation === undefined;
    if (!anonymousUnavailable && (answer.launch_id !== draft.request.launch_id || answer.generation !== draft.request.generation)) {
      diagnostic("queue-identity-missing", id);
      throw new PlacementAdmissionError("invalid-response");
    }
    diagnostic(`queued-${answer.reason.toLowerCase()}`, id);
    throw new PlacementQueueError(draft.request, answer);
  }
  // PLACE is not authority until native create-fence and registration proof exists.
  diagnostic("native-create-proof-unavailable", id);
  throw new PlacementAdmissionError("unavailable");
}
