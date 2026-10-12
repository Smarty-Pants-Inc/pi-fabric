import { Type } from "typebox";
import { Value } from "typebox/value";
import {
  compactionRequestBoundsError,
  encodeCompactionRequest,
  MAX_COMPACTION_INSTRUCTIONS_CHARS,
  MAX_PRESERVE_ITEM_CHARS,
  MAX_PRESERVE_ITEMS,
} from "../compaction/instructions.js";
import {
  applyCarryUpdate,
  COMPACTION_CARRY_ENTRY_TYPE,
  isCarryUpdate,
  latestCarryItems,
  MAX_CARRY_ITEM_CHARS,
  MAX_CARRY_ITEMS,
  sameCarryItems,
  type CompactionCarryEntryData,
  type CompactionCarryUpdate,
} from "../compaction/carry.js";
import { ownerFromContext } from "../compaction/owner.js";
import { compactionPressure } from "../compaction/pressure.js";
import { DEFAULT_FABRIC_CONFIG, type FabricConfig } from "../config.js";
import { CompactController } from "../core/compact-controller.js";
import type {
  FabricActionDescriptor,
  FabricInvocationContext,
  FabricProvider,
  FabricProviderListRequest,
} from "../protocol.js";
import { actionArgNormalizer } from "./arg-normalization.js";

// Fabric provider exposing the host-session compaction controller to
// `fabric_exec`. Compaction is advisory-then-committed: `request` only records
// an intent the host commits at the next `agent_settled` boundary; the model
// cannot compact the running context directly. Always available (no config
// guard) — it is a first-principles primitive, not an optional capability.

const requestSchema = Type.Object({
  reason: Type.Optional(Type.String({
    maxLength: 1024,
    description: "Short human-readable reason for the compaction",
  })),
  instructions: Type.Optional(Type.String({
    maxLength: MAX_COMPACTION_INSTRUCTIONS_CHARS,
    description: "Custom compaction instructions forwarded to Pi core",
  })),
  preserve: Type.Optional(Type.Array(
    Type.String({ maxLength: MAX_PRESERVE_ITEM_CHARS }),
    {
      maxItems: MAX_PRESERVE_ITEMS,
      description: "Explicit bounded facts to preserve, encoded as a typed Fabric compaction request",
    },
  )),
  requestedBy: Type.Optional(Type.String({
    maxLength: 256,
    description: "Who requested the compaction (default: model)",
  })),
}, { additionalProperties: false });

interface CompactRequestArguments {
  reason?: string;
  instructions?: string;
  preserve?: string[];
  requestedBy?: string;
}

const checkedRequestArguments = (args: Record<string, unknown>): CompactRequestArguments => {
  if (!Value.Check(requestSchema, args)) {
    const message = [...Value.Errors(requestSchema, args)]
      .slice(0, 5)
      .map((error) => error.message)
      .join("; ");
    throw new Error(`Invalid compact.request arguments: ${message}`);
  }
  const input = args as CompactRequestArguments;
  const boundsError = compactionRequestBoundsError({
    ...(input.instructions !== undefined ? { instructions: input.instructions } : {}),
    ...(input.preserve !== undefined ? { preserve: input.preserve } : {}),
  });
  if (boundsError) throw new Error(`Invalid compact.request arguments: ${boundsError.message}`);
  if (input.preserve !== undefined) {
    encodeCompactionRequest({
      ...(input.instructions !== undefined ? { instructions: input.instructions } : {}),
      preserve: input.preserve,
    });
  }
  return input;
};

const emptySchema = {
  type: "object",
  properties: {},
  additionalProperties: false,
};

const carryItemsSchema = (description: string) => Type.Optional(Type.Array(
  Type.String({ minLength: 1, maxLength: MAX_CARRY_ITEM_CHARS }),
  { maxItems: MAX_CARRY_ITEMS, description },
));

const carrySchema = Type.Object({
  items: carryItemsSchema("Replace the carry-forward list"),
  add: carryItemsSchema("Append items not already present"),
  remove: carryItemsSchema("Remove exact items"),
  clear: Type.Optional(Type.Boolean({ description: "Empty the list before applying items/add" })),
}, { additionalProperties: false });

const checkedCarryArguments = (args: Record<string, unknown>): CompactionCarryUpdate => {
  if (!Value.Check(carrySchema, args)) {
    const message = [...Value.Errors(carrySchema, args)]
      .slice(0, 5)
      .map((error) => error.message)
      .join("; ");
    throw new Error(`Invalid compact.carry arguments: ${message}`);
  }
  return args as CompactionCarryUpdate;
};



const descriptors: FabricActionDescriptor[] = [
  {
    name: "request",
    description:
      "Request an advisory compaction of the host session's context at the next safe boundary (agent_settled). The host commits it only between turns, never mid-turn. A new request replaces any pending one.",
    inputSchema: requestSchema as unknown as Record<string, unknown>,
    risk: "write",
  },
  {
    name: "status",
    description:
      "Read the pending compaction intent and the last compaction outcome",
    inputSchema: emptySchema,
    risk: "read",
  },
  {
    name: "pressure",
    description:
      "Read host context pressure: tokens, window, fraction, headroom, band (ok/warn/urgent/unknown), output reserve, configured threshold, and the observed compaction owner",
    inputSchema: emptySchema,
    risk: "read",
  },
  {
    name: "carry",
    description:
      "Read or update the persistent carry-forward focus list that Fabric's compactor renders in every summary until cleared. No arguments reads it.",
    inputSchema: carrySchema as unknown as Record<string, unknown>,
    // A persisted session entry that changes every later summary: a control
    // write, classified like request.
    risk: "write",
  },
  {
    name: "cancel",
    description: "Clear a pending compaction intent before the host commits it",
    inputSchema: emptySchema,
    // Cancel mutates the compaction controller; it is not a read. The "read"
    // label predates effect tracking and would have let speculative PTC
    // pre-fire a control action that may never execute in the real program.
    risk: "write",
  },
];

// Argument repair derives from the action schemas plus the shared synonym
// lexicon; no compact-specific table remains.
export const normalizeCompactArgs = actionArgNormalizer(() => descriptors);

type CompactionConfig = FabricConfig["compaction"];

export interface CompactProviderOptions {
  /** Live compaction config (bands, output reserve, thresholds). */
  config?: () => CompactionConfig;
  /** Session custom-entry writer (`pi.appendEntry`); required for carry updates. */
  appendEntry?: (customType: string, data: CompactionCarryEntryData) => void;
}

const branchOf = (context: FabricInvocationContext) => {
  try {
    const branch = context.extensionContext?.sessionManager?.getBranch?.();
    return Array.isArray(branch) ? branch : undefined;
  } catch {
    return undefined;
  }
};

export class CompactProvider implements FabricProvider {
  readonly name = "compact";
  readonly description =
    "Programmatic, advisory-then-committed context compaction for the host Pi session";

  constructor(
    readonly controller: CompactController,
    private readonly options: CompactProviderOptions = {},
  ) {}

  #config(): CompactionConfig {
    return this.options.config?.() ?? DEFAULT_FABRIC_CONFIG.compaction;
  }

  #carry(update: CompactionCarryUpdate, context: FabricInvocationContext): { items: string[] } {
    const branch = branchOf(context);
    const current = branch ? latestCarryItems(branch) : [];
    if (!isCarryUpdate(update)) return { items: current };
    if (!branch || !this.options.appendEntry) {
      throw new Error("compact.carry cannot persist: no host session is available");
    }
    const items = applyCarryUpdate(current, update);
    if (!sameCarryItems(current, items)) {
      this.options.appendEntry(COMPACTION_CARRY_ENTRY_TYPE, { version: 1, items });
      context.activity?.({
        type: "progress",
        message: items.length > 0
          ? `Compaction carry-forward: ${items.length} item${items.length === 1 ? "" : "s"}`
          : "Compaction carry-forward cleared",
      });
    }
    return { items };
  }

  async list(
    request: FabricProviderListRequest,
    _context: FabricInvocationContext,
  ): Promise<FabricActionDescriptor[]> {
    const query = request.query?.toLowerCase();
    return query
      ? descriptors.filter((descriptor) =>
          `${descriptor.name} ${descriptor.description}`.toLowerCase().includes(query),
        )
      : descriptors;
  }

  async describe(
    actionName: string,
    _context: FabricInvocationContext,
  ): Promise<FabricActionDescriptor | undefined> {
    return descriptors.find((descriptor) => descriptor.name === actionName);
  }

  prepareArguments(
    actionName: string,
    args: Record<string, unknown>,
  ): Record<string, unknown> {
    return normalizeCompactArgs(actionName, args);
  }

  async invoke(
    actionName: string,
    args: Record<string, unknown>,
    context: FabricInvocationContext,
  ): Promise<unknown> {
    switch (actionName) {
      case "request": {
        const input = checkedRequestArguments(args);
        const intent = this.controller.request({
          ...(input.reason !== undefined ? { reason: input.reason } : {}),
          ...(input.instructions !== undefined ? { instructions: input.instructions } : {}),
          ...(input.preserve !== undefined ? { preserve: input.preserve } : {}),
          ...(input.requestedBy !== undefined ? { requestedBy: input.requestedBy } : {}),
        });
        context.activity?.({
          type: "entity",
          id: "host-compact",
          kind: "custom",
          name: "Context compaction",
        });
        context.activity?.({
          type: "progress",
          message: intent.reason
            ? `Compaction requested: ${intent.reason}`
            : "Compaction requested (advisory; commits at next agent_settled)",
        });
        return { requested: true, intent };
      }
      case "status":
        return {
          ...this.controller.status(),
          owner: ownerFromContext(context.extensionContext),
          outputReserveTokens: this.#config().outputReserveTokens,
        };
      case "pressure":
        return compactionPressure(context.extensionContext, this.#config());
      case "carry":
        return this.#carry(checkedCarryArguments(args), context);
      case "cancel":
        this.controller.cancel();
        context.activity?.({ type: "progress", message: "Compaction request cancelled" });
        return { cancelled: true };
      default:
        throw new Error(`Unknown compact action: ${actionName}`);
    }
  }
}
