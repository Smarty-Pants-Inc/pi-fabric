import type { ExtensionContext, ExtensionRunner, RegisteredTool, ToolDefinition } from "@earendil-works/pi-coding-agent";

// Resident work accounting is needed only once a captured tool runs. Load it at
// first use so it stays out of the startup static graph; its counters live on
// globalThis, so every importer shares one ledger.
let processWorkLoad: Promise<typeof import("../residency/process-work.js")> | undefined;
const loadProcessWork = () => processWorkLoad ??= import("../residency/process-work.js")
  .catch((error: unknown) => { processWorkLoad = undefined; throw error; });

// Local mirror of wrapRegisteredTool/wrapToolDefinition (pi 0.99.0,
// core/extensions/wrapper.js and core/tools/tool-definition-wrapper.js).
// Captured tools must execute with exactly the host wrapper semantics —
// extension runner context injection and post-execution addedToolNames merge —
// without importing the host package during extension load.

type WrappedExecute = (
  toolCallId: unknown,
  params: unknown,
  signal: unknown,
  onUpdate: (update: any) => void,
  ctx?: unknown,
) => Promise<any>;

export interface WrappedRegisteredTool {
  name: string;
  label: string | undefined;
  description: string | undefined;
  parameters: unknown;
  outputSchema: unknown;
  constrainedSampling: unknown;
  prepareArguments: ((args: Record<string, unknown>) => unknown) | undefined;
  executionMode: unknown;
  execute: WrappedExecute;
}

const wrapToolDefinition = (
  definition: ToolDefinition<any, any, any>,
  ctxFactory: (toolCallId: unknown, signal: unknown) => unknown,
): WrappedRegisteredTool => {
  const execute = definition.execute as unknown as WrappedExecute;
  return {
    name: definition.name,
    label: definition.label,
    description: definition.description,
    parameters: definition.parameters,
    outputSchema: definition.outputSchema,
    constrainedSampling: definition.constrainedSampling,
    prepareArguments: definition.prepareArguments,
    executionMode: definition.executionMode,
    execute: (toolCallId, params, signal, onUpdate, ctx) =>
      execute(toolCallId, params, signal, onUpdate, ctx ?? ctxFactory(toolCallId, signal)),
  };
};

export const wrapRegisteredToolForCapture = (
  registeredTool: RegisteredTool,
  runner: ExtensionRunner,
  onToolsRemoved?: (names: string[]) => void,
): WrappedRegisteredTool => {
  // Host wrapper.js uses createToolContext(id, signal) (provides ctx.tools);
  // createContext() is only the legacy fallback for older runners.
  const ctxFactory = (id: unknown, signal: unknown) => runner.createToolContext
    ? runner.createToolContext(id as string, signal as AbortSignal | undefined)
    : runner.createContext();
  const tool = wrapToolDefinition(
    registeredTool.definition as ToolDefinition<any, any, any>,
    ctxFactory,
  );
  const execute = tool.execute;
  return {
    ...tool,
    execute: async (toolCallId, params, signal, onUpdate, ctx): Promise<any> => {
      const invocationSignal = signal as AbortSignal | undefined;
      invocationSignal?.throwIfAborted();
      const { retainResidentProcessWork } = await loadProcessWork();
      // Never start a tool whose caller aborted while the ledger loaded.
      invocationSignal?.throwIfAborted();
      // Explicit caller ctx wins; otherwise the per-call native tool context.
      const context = (ctx ?? ctxFactory(toolCallId, signal)) as ExtensionContext;
      const releaseWork = retainResidentProcessWork(context.sessionManager?.getSessionId?.());
      try {
        const activeBefore = runner.getActiveTools();
        const result = await execute(toolCallId, params, signal, onUpdate, context);
        // A browser/process tool can settle after reload canceled its caller. Never
        // read the retired runner or merge its tools into a successor (#5962).
        invocationSignal?.throwIfAborted();
        const activeAfter = runner.getActiveTools();
        const activeAfterNames = new Set(activeAfter);
        const removedToolNames = activeBefore.filter((name) => !activeAfterNames.has(name));
        if (removedToolNames.length > 0) {
          onToolsRemoved?.(removedToolNames);
          return result;
        }
        const beforeNames = new Set(activeBefore);
        const addedToolNames = activeAfter.filter((name) => !beforeNames.has(name));
        if (addedToolNames.length === 0) {
          return result;
        }
        const previous = ((result as { addedToolNames?: string[] } | undefined)?.addedToolNames) ?? [];
        return {
          ...(result as Record<string, unknown>),
          addedToolNames: [...new Set([...previous, ...addedToolNames])],
        };
      } finally { releaseWork(); }
    },
  };
};
