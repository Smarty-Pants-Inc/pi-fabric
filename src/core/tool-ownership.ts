import path from "node:path";
import type {
  ToolCallEvent,
  ToolCallEventResult,
  ToolResultEvent,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { readFabricExecutionTraceV1 } from "../audit/index.js";
import { FABRIC_NESTED_TOOL_CALL_ID_PREFIX as NESTED_TOOL_CALL_ID_PREFIX } from "../protocol.js";
import { REPLY_TOOL_NAME } from "./reply-tool-identity.js";
import type { ToolLoadout, ToolLoadoutChanges } from "@earendil-works/pi-coding-agent";

// Hide every registered declaration, including historical transcript declarations.
// Keep tools active: Pi 0.99 uses that set for native nested-call availability.
export const fabricToolLoadout = (
  loadout: ToolLoadout,
  exclusive: boolean,
  visible: ReadonlySet<string> = new Set(["fabric_exec"]),
): ToolLoadoutChanges | undefined =>
  exclusive ? { hiddenDeclarations: loadout.registered.map((tool) => tool.name).filter((name) => name !== "fabric_exec" && !visible.has(name)) } : undefined;

// setActiveTools can run during a captured call or in a later extension handler,
// removing this tool (and thus its loadout hook). Reassert at the native request
// boundary and project the whole transcript, including historical removals.
export const fabricModelContext = (
  messages: import("@earendil-works/pi-agent-core").AgentMessage[],
  tool: Pick<import("@earendil-works/pi-ai").Tool, "name" | "description" | "parameters">,
  visible: ReadonlyArray<Pick<import("@earendil-works/pi-ai").Tool, "name" | "description" | "parameters">> = [],
): import("@earendil-works/pi-agent-core").AgentMessage[] => {
  let first = true;
  return messages.map((message) => {
    if (message.role !== "system") return message;
    const { toolsAdded: _added, toolsRemoved: _removed, ...rest } = message;
    const declare = first || ("replace" in message && message.replace === true);
    first = false;
    return { ...rest, ...(declare ? { toolsAdded: [tool, ...visible] } : {}) };
  });
};

export interface FabricToolOwnershipHost {
  getActiveTools(): string[];
  setActiveTools(names: string[]): void;
}

export interface FabricTopLevelToolAuthorizer {
  authorize(ref: string, parentToolCallId: string): Promise<void>;
}

export interface FabricTopLevelToolApprover {
  approve(event: ToolCallEvent, context: ExtensionContext): Promise<void>;
}

const FABRIC_TOOL_NAME = "fabric_exec";
const TOP_LEVEL_SCHEMA_REF_PREFIX = "schema.top_level_tool.";

export const ownsFabricToolSource = (
  tools: Array<{ name: string; sourceInfo: { path: string } }>,
  extensionEntryPath: string,
): boolean => tools.some(
  (tool) =>
    tool.name === FABRIC_TOOL_NAME &&
    path.resolve(tool.sourceInfo.path) === path.resolve(extensionEntryPath),
);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const finalFabricDetailsFailed = (details: unknown): boolean => {
  if (!isRecord(details)) return false;
  if (details.success === false) return true;
  const trace = readFabricExecutionTraceV1(details.trace);
  return trace !== undefined && trace.outcome !== "succeeded";
};

export class FabricToolLifecycle {
  readonly #outerCalls = new Set<string>();

  constructor(
    readonly ownsFabricTool: () => boolean,
    readonly authorizer: () => FabricTopLevelToolAuthorizer | undefined,
    readonly approver: () => FabricTopLevelToolApprover | undefined = () => undefined,
    // The worker's run-local reply tool, verified by its hook file (smarty-dev#967).
    readonly ownsReplyTool: () => boolean = () => false,
  ) {}

  async toolCall(
    event: ToolCallEvent,
    context?: ExtensionContext,
  ): Promise<ToolCallEventResult | undefined> {
    if (event.parentToolCallId && [...this.#outerCalls].some((id) =>
      event.parentToolCallId === id || event.parentToolCallId?.startsWith(`${id}/`))) return undefined;
    if (event.toolCallId.startsWith(NESTED_TOOL_CALL_ID_PREFIX)) {
      if (this.#outerCalls.size > 0) return undefined;
      await this.#authorizeTopLevel(event);
      return undefined;
    }
    if (event.toolName === FABRIC_TOOL_NAME && this.ownsFabricTool()) {
      this.#outerCalls.add(event.toolCallId);
      return undefined;
    }
    if (event.toolName === REPLY_TOOL_NAME && this.ownsReplyTool()) return undefined;
    await this.#authorizeTopLevel(event);
    const approver = this.approver();
    if (approver) {
      if (!context) throw new Error("Fabric direct tool approval needs an extension context");
      await approver.approve(event, context);
    }
    return undefined;
  }

  toolResult(event: ToolResultEvent): { isError: true } | undefined {
    if (
      event.toolName !== FABRIC_TOOL_NAME ||
      event.toolCallId.startsWith(NESTED_TOOL_CALL_ID_PREFIX) ||
      !this.#outerCalls.delete(event.toolCallId)
    ) {
      return undefined;
    }
    return !event.isError && finalFabricDetailsFailed(event.details)
      ? { isError: true }
      : undefined;
  }

  clear(): void {
    this.#outerCalls.clear();
  }

  async #authorizeTopLevel(event: ToolCallEvent): Promise<void> {
    await this.authorizer()?.authorize(
      `${TOP_LEVEL_SCHEMA_REF_PREFIX}${event.toolName}`,
      event.toolCallId,
    );
  }
}

export interface ToolOwnershipReassertion {
  reassert(): void;
  schedule(): void;
}

// Re-asserts active-set ownership after registry refreshes and at turn
// boundaries. Refresh-driven microtasks can run before the host finished
// initializing (registry rebuilds happen before session_start), when neither
// the live config nor the active tool set is safe to touch — `ready` guards
// every entry point, including the deferred microtask.
export const createToolOwnershipReassertion = (options: {
  ready: () => boolean;
  active: () => boolean;
  hiddenNames: () => ReadonlySet<string>;
  apply: (hidden: ReadonlySet<string>) => boolean;
}): ToolOwnershipReassertion => {
  let queued = false;
  const reassert = (): void => {
    queued = false;
    if (!options.ready() || !options.active()) return;
    options.apply(options.hiddenNames());
  };
  return {
    reassert,
    schedule: () => {
      if (queued) return;
      queued = true;
      queueMicrotask(reassert);
    },
  };
};

export class FabricToolOwnership {
  constructor(readonly host: FabricToolOwnershipHost) {}

  apply(fullCodeMode: boolean, _hiddenExtensionTools?: ReadonlySet<string>): boolean {
    if (!fullCodeMode) return false;
    const active = this.host.getActiveTools();
    if (active.includes(FABRIC_TOOL_NAME)) return false;
    this.host.setActiveTools([...active, FABRIC_TOOL_NAME]);
    return true;
  }

  // Native prepareLoadout owns visibility. No active tools were removed, so
  // leaving full-code mode/shutting down has no saved selection to restore.
  release(): boolean { return false; }
}
