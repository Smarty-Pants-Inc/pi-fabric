import { validationMessage } from "../core/action-arguments.js";
import type { FabricActionDescriptor, FabricInvocationContext, FabricProvider, FabricProviderListRequest } from "../protocol.js";
import { THINKING_LEVELS } from "../thinking.js";
import {
  MAX_THINKING_OVERRIDE_TURNS,
  MAX_THINKING_REASON_CHARS,
  type FabricThinkingController,
  type FabricThinkingSetInput,
} from "../thinking-control.js";
import { actionArgNormalizer } from "./arg-normalization.js";

// Host-session thinking (reasoning effort) control for `fabric_exec`.
// Always available outside managed hosts: a first-principles primitive like
// compact.*, not an optional capability. Levels are clamped into
// `thinking.bounds` and the active model's supported levels.

const emptySchema = { type: "object", properties: {}, additionalProperties: false };
const setSchema = {
  type: "object",
  required: ["level"],
  additionalProperties: false,
  properties: {
    level: { type: "string", enum: [...THINKING_LEVELS], description: "Requested level; clamped into thinking.bounds and the model's supported levels" },
    scope: {
      type: "string", enum: ["turn", "turns", "session"],
      description: "turn (default) reverts at the next agent_end; turns reverts after `turns` agent_end events (the current run counts); session lasts until thinking.reset()",
    },
    turns: { type: "integer", minimum: 1, maximum: MAX_THINKING_OVERRIDE_TURNS, description: "Required with scope \"turns\"; invalid otherwise" },
    reason: { type: "string", maxLength: MAX_THINKING_REASON_CHARS },
  },
};
const resource = ["pi:thinking-level"];
const descriptors: FabricActionDescriptor[] = [
  {
    name: "status",
    description: "Read the host session's thinking level, model-supported levels inside the effective bounds, the baseline, and any active Fabric override",
    inputSchema: emptySchema, risk: "read",
    effect: { kind: "none", resources: resource, ordering: "commutative" },
  },
  {
    name: "set",
    description: "Temporarily change the host session's thinking level for this turn, N turns, or the session. Clamped into bounds and model support (reports clamped/requested). Reverts to the baseline at agent_end; restored after reload.",
    inputSchema: setSchema, risk: "write",
    effect: { kind: "emission", resources: resource, ordering: "ordered" },
  },
  {
    name: "reset",
    description: "End any Fabric thinking override and restore the baseline level",
    inputSchema: emptySchema, risk: "write",
    effect: { kind: "emission", resources: resource, ordering: "ordered" },
  },
];

export const normalizeThinkingArgs = actionArgNormalizer(() => descriptors);

export class ThinkingProvider implements FabricProvider {
  readonly name = "thinking";
  readonly description = "Bounded host-session thinking (reasoning effort) control";

  constructor(
    private readonly controller: FabricThinkingController,
    private readonly sessionId: string,
  ) {}

  async list(request: FabricProviderListRequest): Promise<FabricActionDescriptor[]> {
    const query = request.query?.toLowerCase();
    return descriptors.filter((action) => !query || `${action.name} ${action.description}`.toLowerCase().includes(query));
  }

  async describe(name: string): Promise<FabricActionDescriptor | undefined> {
    return descriptors.find((action) => action.name === name);
  }

  prepareArguments(name: string, args: Record<string, unknown>): Record<string, unknown> {
    return normalizeThinkingArgs(name, args);
  }

  async invoke(name: string, args: Record<string, unknown>, context: FabricInvocationContext): Promise<unknown> {
    const descriptor = descriptors.find((action) => action.name === name);
    if (!descriptor) throw new Error(`Unknown thinking action: ${name}`);
    const error = validationMessage(descriptor.inputSchema, args);
    if (error) throw new Error(`Invalid thinking.${name} arguments: ${error}`);
    const host = context.extensionContext;
    if (host.sessionManager.getSessionId() !== this.sessionId) {
      throw new Error("Thinking provider belongs to a different session");
    }
    if (name === "status") return this.controller.status(host);
    if (name === "reset") {
      const status = this.controller.reset(host);
      context.activity?.({ type: "progress", message: `Thinking level reset to ${status.level}` });
      return status;
    }
    const result = this.controller.set(args as unknown as FabricThinkingSetInput, host);
    context.activity?.({
      type: "progress",
      message: result.clamped
        ? `Thinking level ${result.level} (requested ${result.requested}, clamped)`
        : `Thinking level ${result.level} (${result.override?.scope ?? "turn"})`,
    });
    return result;
  }
}
