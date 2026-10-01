import path from "node:path";
import type {
  ToolCallEvent,
  ToolCallEventResult,
  ToolResultEvent,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { readFabricExecutionTraceV1 } from "../audit/index.js";
import { FABRIC_NESTED_TOOL_CALL_ID_PREFIX as NESTED_TOOL_CALL_ID_PREFIX } from "../protocol.js";
import { PI_CORE_TOOL_NAME_SET } from "./pi-tools.js";
import { REPLY_TOOL_NAME } from "./reply-tool-identity.js";

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
// Pi 0.99+ native orchestrators compete with fabric_exec in exclusive mode.
// Keep them registered, but never model-visible, even with capture disabled or
// a keepVisible override. Native MCP/deferred tools can otherwise widen the
// loadout or run a second sandbox outside Fabric's execution policy.
const NATIVE_ORCHESTRATOR_NAMES: ReadonlySet<string> = new Set(["codemode", "tool_search"]);
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
    readonly exclusive: () => boolean = () => false,
  ) {}

  async toolCall(
    event: ToolCallEvent,
    context?: ExtensionContext,
  ): Promise<ToolCallEventResult | undefined> {
    // Native ctx.executeTool calls carry parentToolCallId. They have not gone
    // through Fabric's registry/schema/approval pipeline, even when their id
    // inherits our prefix from a captured caller. Do not grant them the
    // already-authorized Fabric nested-call exemption.
    const nativeNested = "parentToolCallId" in event && typeof event.parentToolCallId === "string";
    if (event.toolCallId.startsWith(NESTED_TOOL_CALL_ID_PREFIX) && !nativeNested) {
      if (this.#outerCalls.size > 0) return undefined;
      await this.#authorizeTopLevel(event);
      return undefined;
    }
    if (this.exclusive() && NATIVE_ORCHESTRATOR_NAMES.has(event.toolName)) {
      // Registry refresh/MCP auto-activation can race loadout construction.
      // Fail closed at execution too, even if a stale declaration escaped.
      return { block: true, reason: `Native ${event.toolName} is disabled while fabric_exec owns full-code or schema-enforce execution` };
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

const sameTools = (left: string[], right: string[]): boolean =>
  left.length === right.length && left.every((name, index) => name === right[index]);

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
  #savedNativeCoreTools: Array<{ name: string; index: number }> | undefined;
  // Captured extension tools stay registered so host extensions (permission
  // systems, auditors) keep them in `pi.getAllTools()`; hiding from the model
  // happens here, in the active set. Removed names are remembered so leaving
  // full code mode (or adding a name to `capture.keepVisible`) re-exposes them.
  #savedHiddenExtensionTools = new Map<string, number>();

  constructor(readonly host: FabricToolOwnershipHost) {}

  apply(fullCodeMode: boolean, hiddenExtensionTools?: ReadonlySet<string>): boolean {
    const active = this.host.getActiveTools();
    if (!fullCodeMode) return this.#restore(active);

    this.#savedNativeCoreTools ??= active.flatMap((name, index) =>
      PI_CORE_TOOL_NAME_SET.has(name) ? [{ name, index }] : [],
    );
    const hidden = new Set([
      ...(hiddenExtensionTools ?? []),
      ...NATIVE_ORCHESTRATOR_NAMES,
    ]);
    const next: string[] = [];
    active.forEach((name, index) => {
      if (PI_CORE_TOOL_NAME_SET.has(name)) return;
      if (hidden.has(name)) {
        if (!this.#savedHiddenExtensionTools.has(name)) {
          this.#savedHiddenExtensionTools.set(name, index);
        }
        return;
      }
      next.push(name);
    });
    for (const [name, index] of this.#savedHiddenExtensionTools) {
      if (hidden.has(name) || next.includes(name)) continue;
      this.#savedHiddenExtensionTools.delete(name);
      next.splice(Math.min(index, next.length), 0, name);
    }
    if (!next.includes("fabric_exec")) next.push("fabric_exec");
    return this.#setIfChanged(active, next);
  }

  release(): boolean {
    return this.#restore(this.host.getActiveTools());
  }

  #restore(active: string[]): boolean {
    const saved = this.#savedNativeCoreTools;
    const savedHidden = this.#savedHiddenExtensionTools;
    if (!saved && savedHidden.size === 0) return false;
    this.#savedNativeCoreTools = undefined;
    this.#savedHiddenExtensionTools = new Map();
    const next = [...active];
    for (const { name, index } of saved ?? []) {
      if (!next.includes(name)) next.splice(Math.min(index, next.length), 0, name);
    }
    for (const [name, index] of savedHidden) {
      if (!next.includes(name)) next.splice(Math.min(index, next.length), 0, name);
    }
    return this.#setIfChanged(active, next);
  }

  #setIfChanged(active: string[], next: string[]): boolean {
    if (sameTools(active, next)) return false;
    this.host.setActiveTools(next);
    return true;
  }
}
