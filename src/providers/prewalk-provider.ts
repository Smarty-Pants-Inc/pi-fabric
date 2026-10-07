import type {
  FabricActionDescriptor,
  FabricInvocationContext,
  FabricProvider,
  FabricProviderListRequest,
} from "../protocol.js";
import type { PrewalkController } from "../prewalk/controller.js";
import { checkedPrewalkPlan, prewalkPlanSchema, prewalkPlanText } from "../prewalk/plan.js";
import { runtimeIdentityStatus, type FabricRuntimeIdentity } from "../build-identity.js";
import { actionArgNormalizer } from "./arg-normalization.js";

// Fabric provider exposing prewalk readiness to fabric_exec. The recorded plan
// is the gate: a boundary reached while the arm still owes one is withheld
// instead of handing off. The controller owns the plan and snapshots it into
// the handoff; executor delivery does not depend on returning this action's value.

export interface PrewalkProviderOptions {
  // Loaded-code provenance for the extension entry and lazy runtime module.
  buildIdentity?: () => FabricRuntimeIdentity;
}

const emptySchema = { type: "object", properties: {}, additionalProperties: false };

const descriptors: FabricActionDescriptor[] = [
  {
    name: "plan",
    description:
      "Submit the executor handoff plan only when this session is awaiting one and Fabric's armed advisory or checkpoint requests it. Not a general task planner, checklist, or acceptance-ledger store. Include the outcome, ordered steps with exact files and checks, verification, and risks. Fabric delivers the recorded plan directly to the executor even if this action's return is discarded.",
    inputSchema: prewalkPlanSchema as unknown as Record<string, unknown>,
    risk: "write",
  },
  {
    name: "status",
    description:
      "Report the prewalk arm, plan readiness, the readiness kind claimed by an in-flight handoff, and the loaded build identity of the extension entry and lazy runtime versus the files currently on disk",
    inputSchema: emptySchema,
    risk: "read",
  },
];

// Argument repair derives from the action schemas plus the shared synonym
// lexicon; no prewalk-specific table remains.
export const normalizePrewalkArgs = actionArgNormalizer(() => descriptors);

export class PrewalkProvider implements FabricProvider {
  readonly name = "prewalk";
  readonly description =
    "Prewalk executor handoff readiness; not general task planning";

  constructor(
    readonly controller: PrewalkController,
    readonly options: PrewalkProviderOptions = {},
  ) {}

  async list(
    request: FabricProviderListRequest,
    context: FabricInvocationContext,
  ): Promise<FabricActionDescriptor[]> {
    const query = request.query?.toLowerCase();
    const planRequired = this.controller.planRequired(
      context.extensionContext.sessionManager.getSessionId(),
    );
    return descriptors.filter((descriptor) =>
      (descriptor.name !== "plan" || planRequired) &&
      (!query || `${descriptor.name} ${descriptor.description}`.toLowerCase().includes(query)),
    );
  }

  async describe(
    actionName: string,
    context: FabricInvocationContext,
  ): Promise<FabricActionDescriptor | undefined> {
    return (await this.list({}, context)).find((descriptor) => descriptor.name === actionName);
  }

  prepareArguments(
    actionName: string,
    args: Record<string, unknown>,
  ): Record<string, unknown> {
    return normalizePrewalkArgs(actionName, args);
  }

  async invoke(
    actionName: string,
    args: Record<string, unknown>,
    context: FabricInvocationContext,
  ): Promise<unknown> {
    const sessionId = context.extensionContext.sessionManager.getSessionId();
    switch (actionName) {
      case "plan": {
        const plan = checkedPrewalkPlan(args);
        if (!this.controller.submitPlan(sessionId, plan)) {
          throw new Error(
            "Prewalk is not awaiting a plan for this session: the arm is idle, already planned, or the gate is off",
          );
        }
        context.activity?.({
          type: "entity",
          id: "prewalk-plan",
          kind: "custom",
          name: "Prewalk plan recorded",
        });
        context.update("Prewalk plan recorded; the next mutation hands off to the executor");
        return { recorded: true, readiness: "ready", plan: prewalkPlanText(plan) };
      }
      case "status": {
        const status = this.controller.status();
        const plan = this.controller.planState(sessionId);
        const claimed =
          (status.state === "handing_off" || status.state === "continuation_pending") &&
          status.sessionId === sessionId
            ? status.claimedReadiness
            : undefined;
        return {
          state: status.state,
          planRequired: plan.required,
          planReady: plan.ready,
          planPrompts: plan.prompts,
          // The claim snapshot's readiness kind: distinguishes a plan that was
          // recorded and then consumed by a handoff from an arm that never
          // recorded one (F4 of the live dogfood run).
          claimedReadiness: claimed ?? null,
          ...(this.options.buildIdentity
            ? { runtime: runtimeIdentityStatus(this.options.buildIdentity()) }
            : {}),
        };
      }
      default:
        throw new Error(`Unknown prewalk action: ${actionName}`);
    }
  }
}
