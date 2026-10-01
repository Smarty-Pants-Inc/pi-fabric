import path from "node:path";
import { AsyncLocalStorage } from "node:async_hooks";
import type {
  ToolCallEvent,
  ToolCallEventResult,
  ToolResultEvent,
  ExtensionContext,
  ToolDefinition,
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

interface OwnedInvocation {
  active: boolean;
  parent?: OwnedInvocation;
}

export class FabricToolLifecycle {
  readonly #ownedCalls = new Map<string, OwnedInvocation>();
  readonly #nestedCalls = new Map<string, OwnedInvocation>();
  // Failure-status repair must survive execute-time authorization revocation.
  readonly #resultCalls = new Set<string>();
  readonly #execution = new AsyncLocalStorage<OwnedInvocation>();

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
    // A call id is never permission to enter a competing native orchestrator.
    // Apply this backstop before *every* prefix/ownership shortcut, even off-schema.
    if (this.exclusive() && NATIVE_ORCHESTRATOR_NAMES.has(event.toolName)) {
      return { block: true, reason: `Native ${event.toolName} is disabled while fabric_exec owns full-code or schema-enforce execution` };
    }
    const nativeNested = "parentToolCallId" in event && typeof event.parentToolCallId === "string";
    const owner = this.#execution.getStore();
    // Async descendants retain their owner token, not a fresh ambient grant.
    // An unrelated live outer call must not resurrect a settled owner's window.
    if (owner && !this.#isLive(owner)) {
      return { block: true, reason: "Fabric invocation authorization has ended" };
    }
    if (event.toolName === FABRIC_TOOL_NAME && this.ownsFabricTool()) {
      if (nativeNested) {
        const parentId = event.parentToolCallId as string;
        const parent = this.#ownedCalls.get(parentId) ?? this.#nestedCalls.get(parentId);
        if (!owner || parent !== owner || !this.#isLive(parent)) {
          return { block: true, reason: "Native nested fabric_exec requires a live owned Fabric invocation" };
        }
      }
      this.#ownedCalls.set(event.toolCallId, { active: true, ...(owner ? { parent: owner } : {}) });
      this.#resultCalls.add(event.toolCallId);
      return undefined;
    }
    // Native ctx.executeTool calls have not passed Fabric's schema/approval
    // pipeline, even when their id inherits the captured caller's prefix.
    if (event.toolCallId.startsWith(NESTED_TOOL_CALL_ID_PREFIX) && !nativeNested) {
      if (owner ? this.#isLive(owner) : this.#ownedCalls.size > 0) {
        if (owner) this.#nestedCalls.set(event.toolCallId, owner);
        return undefined;
      }
      await this.#authorizeTopLevel(event);
      return undefined;
    }
    if (event.toolName === REPLY_TOOL_NAME && this.ownsReplyTool()) return undefined;
    await this.#authorizeTopLevel(event);
    if (owner && !this.#isLive(owner)) return { block: true, reason: "Fabric invocation authorization has ended" };
    const approver = this.approver();
    if (approver) {
      if (!context) throw new Error("Fabric direct tool approval needs an extension context");
      await approver.approve(event, context);
    }
    if (owner) {
      if (!this.#isLive(owner)) return { block: true, reason: "Fabric invocation authorization has ended" };
      this.#nestedCalls.set(event.toolCallId, owner);
    }
    return undefined;
  }

  /** Bind the registered execute path, including re-created definitions after reload. */
  bindExecution<T extends ToolDefinition<any, any, any>>(tool: T): T {
    return {
      ...tool,
      execute: (id, args, signal, update, context) =>
        this.runOwned(id, signal, () => tool.execute(id, args, signal, update, context)),
    };
  }

  async runOwned<T>(id: string, signal: AbortSignal | undefined, run: () => Promise<T>): Promise<T> {
    const invocation = this.#ownedCalls.get(id);
    if (!invocation || !this.#isLive(invocation) || signal?.aborted) {
      this.#revoke(id);
      throw new Error("Fabric invocation authorization has ended");
    }
    const abort = (): void => this.#revoke(id);
    signal?.addEventListener("abort", abort, { once: true });
    try {
      return await this.#execution.run(invocation, run);
    } finally {
      signal?.removeEventListener("abort", abort);
      this.#revoke(id);
    }
  }

  toolResult(event: ToolResultEvent): { isError: true } | undefined {
    this.#nestedCalls.delete(event.toolCallId);
    if (event.toolName !== FABRIC_TOOL_NAME || !this.#resultCalls.delete(event.toolCallId)) return undefined;
    // Native prefixed children were admitted too: insertion/cleanup is symmetric.
    this.#revoke(event.toolCallId);
    return !event.isError && finalFabricDetailsFailed(event.details)
      ? { isError: true }
      : undefined;
  }

  clear(): void {
    for (const invocation of this.#ownedCalls.values()) invocation.active = false;
    this.#ownedCalls.clear();
    this.#nestedCalls.clear();
    this.#resultCalls.clear();
  }

  #isLive(invocation: OwnedInvocation): boolean {
    return invocation.active && (!invocation.parent || this.#isLive(invocation.parent));
  }

  #revoke(id: string): void {
    const invocation = this.#ownedCalls.get(id);
    if (!invocation) return;
    invocation.active = false;
    // Revocation cascades immediately; a child that has not returned cannot
    // keep the root alive or authorize later calls from a retained context.
    for (const [callId, grant] of this.#ownedCalls) {
      if (!this.#isLive(grant)) this.#ownedCalls.delete(callId);
    }
    for (const [callId, grant] of this.#nestedCalls) {
      if (!this.#isLive(grant)) this.#nestedCalls.delete(callId);
    }
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
