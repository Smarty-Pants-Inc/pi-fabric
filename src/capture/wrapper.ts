import type { ExtensionRunner, RegisteredTool, ToolDefinition } from "@earendil-works/pi-coding-agent";

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
  const tool = wrapToolDefinition(
    registeredTool.definition as ToolDefinition<any, any, any>,
    (id, signal) => runner.createToolContext
      ? runner.createToolContext(id as string, signal as AbortSignal | undefined)
      : runner.createContext(),
  );
  const execute = tool.execute;
  return {
    ...tool,
    execute: async (toolCallId, params, signal, onUpdate, ctx): Promise<any> => {
      const activeBefore = runner.getActiveTools();
      const result = await execute(toolCallId, params, signal, onUpdate, ctx);
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
    },
  };
};
