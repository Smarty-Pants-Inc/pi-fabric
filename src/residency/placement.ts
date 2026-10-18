import type { ResidentHostConfig } from "./protocol.js";

const baseline = (config: ResidentHostConfig, launchId: string): ResidentHostConfig => ({
  ...config,
  placement: {
    nativeLaunchId: launchId,
    launchSpecId: `resident-host:${config.rootId}`,
    generation: 1,
    fenceToken: 1,
    orgId: config.rootId,
    workId: config.rootId,
    parentNativeLaunchId: null,
    role: "resident-host",
    profile: config.placement?.profile ?? "resident-host",
    sourcePin: config.placement?.sourcePin ?? config.fabricExtensionPath,
    memoryProfile: config.placement?.memoryProfile ?? "resident-host",
    origin: config.placement?.origin ?? { class: "background-agent", originRef: config.sessionId },
    ...(config.placement?.recordRoot ? { recordRoot: config.placement.recordRoot } : {}),
  },
});

/** Resolve one resident host placement at the Main launch boundary. Shadow is deliberately
 * fire-and-forget: it observes the exact request but never withholds the baseline host. */
export const prepareResidentHostPlacement = async (config: ResidentHostConfig, launchId: string): Promise<ResidentHostConfig> => {
  const placementConfig = config.agents.placementDecider;
  const base = baseline(config, launchId);
  if (!placementConfig || placementConfig.mode === "off") return base;
  const [{ PlacementAdmissionError, PlacementQueueError, CliPlacementDecider }, contextModule, contract] = await Promise.all([
    import("../agents/placement-admission.js"), import("../agents/placement-context.js"), import("../agents/placement-contract.js"),
  ]);
  const draftFor = async () => {
    const deadline = performance.now() + contract.PLACEMENT_DEADLINE_MS;
    const context = await contextModule.resolvePlacementCallerContext(placementConfig, deadline);
    return contextModule.buildPlacementRequestDraft(context, {
      launchId, generation: 1, agentName: `fabric-resident-host:${config.rootId}`,
      child: { role: "resident-host", complexity: "normal", profile: base.placement!.profile,
        memoryProfileRef: base.placement!.memoryProfile }, capabilities: [],
    });
  };
  const decider = new CliPlacementDecider(placementConfig);
  if (placementConfig.mode === "shadow") {
    void draftFor().then(draft => decider.observe(draft.request, { deadline: performance.now() + contract.PLACEMENT_DEADLINE_MS })).catch(() => undefined);
    return base;
  }
  const draft = await draftFor();
  if (draft.missingFields.length || !draft.roleAllowed) throw new PlacementAdmissionError("invalid-request");
  const answer = await decider.decide(draft.request, { deadline: performance.now() + contract.PLACEMENT_DEADLINE_MS });
  if (answer.decision === "QUEUE") throw new PlacementQueueError(draft.request, answer);
  return {
    ...config,
    placement: {
      nativeLaunchId: answer.grant.launchId,
      launchSpecId: answer.grant.launchSpecId,
      generation: draft.request.generation,
      fenceToken: answer.grant.fence,
      orgId: draft.request.assignment.org_id,
      workId: draft.request.assignment.work_id,
      parentNativeLaunchId: draft.parentNativeLaunchId ?? null,
      role: "resident-host",
      profile: draft.request.required_profile,
      sourcePin: draft.request.requirements.source_pin,
      memoryProfile: draft.request.requirements.memory_profile_ref,
      origin: { class: draft.request.interaction.class, originRef: draft.request.interaction.origin_ref },
      ...(config.placement?.recordRoot ? { recordRoot: config.placement.recordRoot } : {}),
    },
  };
};