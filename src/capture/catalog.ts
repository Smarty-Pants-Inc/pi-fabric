import type { ExtensionRunner, RegisteredTool, SourceInfo, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { wrapRegisteredToolForCapture } from "./wrapper.js";
import type { FabricToolCaptureConfig } from "../config.js";
import type { FabricRisk } from "../protocol.js";
import { isRunReplyTool } from "../core/reply-tool-identity.js";

export interface CapturedToolEntry {
  name: string;
  definition: ToolDefinition<any, any, any>;
  registeredTool: RegisteredTool;
  sourceInfo: SourceInfo;
  runner: ExtensionRunner;
  wrappedTool: ReturnType<typeof wrapRegisteredToolForCapture>;
  risk: FabricRisk;
}

/** Whether Pi currently offers a registered tool (smarty-dev#5492). */
export type CapturedToolVisibility = (name: string) => boolean;

export class CapturedToolCatalog {
  readonly #tools = new Map<string, CapturedToolEntry>();
  // Pi decides which registered tools it offers: an extension hides a tool by
  // dropping it from the active set (pi.setActiveTools). The source returns a
  // per-read snapshot predicate; undefined (or a throwing host that is not yet
  // initialized) means "no host filter". Fabric must not list, describe, prompt
  // for or invoke a tool its owning extension hid (smarty-dev#5492).
  #hostVisibility: (() => CapturedToolVisibility | undefined) | undefined;
  #visibleSignature: string | undefined;
  readonly #listeners = new Set<() => void>();
  // The ExtensionRunner observed during the last tool refresh. Stored even
  // when capture is disabled so PiToolsProvider can replay the tool-execution
  // lifecycle (tool_call/tool_result/tool_execution_*) for nested pi.* calls
  // in full-code mode — without it, extensions that hook those events
  // (pi-vision-handoff, auditors, etc.) would never fire for pi core tools.
  #runner: ExtensionRunner | undefined;
  #suspended = false;

  get runner(): ExtensionRunner | undefined {
    return this.#runner;
  }

  // True while capture is suspended between sessions/reloads. Derived
  // surfaces (the repair catalog digest) must freeze: the empty catalog is
  // transient and refills with the same tools on re-arm.
  get suspended(): boolean {
    return this.#suspended;
  }

  markSuspended(): void {
    this.#suspended = true;
  }

  markResumed(): void {
    this.#suspended = false;
  }

  replace(
    registeredTools: RegisteredTool[],
    runner: ExtensionRunner,
    config: FabricToolCaptureConfig,
    ownSourcePath: string,
  ): void {
    // Always remember the runner (see field comment) before the enabled gate.
    this.#runner = runner;
    this.#tools.clear();
    if (!config.enabled) {
      // A replace under a disabled policy keeps the suspension flag: during
      // /reload the hub listener fires while capture is still suspended, and
      // that empty catalog is transient, not a stable catalog change.
      this.#emit();
      return;
    }
    this.#suspended = false;

    for (const registeredTool of registeredTools) {
      const { definition, sourceInfo } = registeredTool;
      if (sourceInfo.path === ownSourcePath) continue;
      if (isRunReplyTool(definition.name, sourceInfo.path)) continue;     // host-owned (smarty-dev#967)
      this.#tools.set(definition.name, {
        name: definition.name,
        definition,
        registeredTool,
        sourceInfo,
        runner,
        wrappedTool: wrapRegisteredToolForCapture(registeredTool, runner, (names) => {
          this.remove(names);
        }),
        risk: config.risks[definition.name] ?? config.defaultRisk,
      });
    }
    this.#emit();
  }

  remove(names: Iterable<string>): void {
    let changed = false;
    for (const name of names) changed = this.#tools.delete(name) || changed;
    if (changed) this.#emit();
  }

  clear(): void {
    this.#tools.clear();
    this.#emit();
  }

  // Re-run the capture pass against the last observed runner. During /reload
  // the hub listener fires while capture is still suspended, so the catalog
  // replaces with enabled:false and ends up empty once session_start
  // re-enables it (#73). This forces a fresh replace with the active policy
  // without waiting for pi to call getAllRegisteredTools() again.
  refresh(): void {
    this.#runner?.getAllRegisteredTools();
  }

  subscribe(listener: () => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  setHostVisibility(source: (() => CapturedToolVisibility | undefined) | undefined): void {
    this.#hostVisibility = source;
    this.#visibleSignature = undefined;
  }

  // Re-read the host's offered set and notify observers (tools.catalog,
  // provider bindings) when the visible catalog changed without a registry
  // refresh, e.g. an extension toggled a tool off. Returns whether it emitted.
  revalidateHostVisibility(): boolean {
    const signature = this.list().map((entry) => entry.name).join("\n");
    const previous = this.#visibleSignature;
    this.#visibleSignature = signature;
    if (previous === undefined || previous === signature) return false;
    this.#emit();
    return true;
  }

  // Visible to the model only when Pi still offers it (see #hostVisibility).
  get(name: string): CapturedToolEntry | undefined {
    const tool = this.#tools.get(name);
    if (!tool) return undefined;
    const visible = this.#visibility();
    return !visible || visible(name) ? tool : undefined;
  }

  require(name: string): CapturedToolEntry {
    const tool = this.#tools.get(name);
    if (!tool) throw new Error(`Unknown captured extension tool: ${name}`);
    const visible = this.#visibility();
    if (visible && !visible(name)) {
      throw new Error(`Extension tool ${name} is hidden by its extension (not in Pi's active tool set)`);
    }
    return tool;
  }

  list(): CapturedToolEntry[] {
    const visible = this.#visibility();
    return this.listRegistered().filter((entry) => !visible || visible(entry.name));
  }

  // Every captured registration, including tools the host currently hides.
  // For Fabric's own bookkeeping (active-set ownership, repair digest, UI
  // rendering of past calls), never for model-facing listings or prompts.
  listRegistered(): CapturedToolEntry[] {
    return [...this.#tools.values()].sort((left, right) => left.name.localeCompare(right.name));
  }

  getRegistered(name: string): CapturedToolEntry | undefined {
    return this.#tools.get(name);
  }

  get size(): number {
    return this.list().length;
  }

  #visibility(): CapturedToolVisibility | undefined {
    try {
      return this.#hostVisibility?.();
    } catch {
      return undefined;
    }
  }

  #emit(): void {
    for (const listener of [...this.#listeners]) {
      try { listener(); } catch { /* Catalog observers cannot interrupt capture refresh. */ }
    }
  }
}
