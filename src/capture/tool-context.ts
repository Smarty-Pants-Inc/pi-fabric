import type { ExtensionContext, ExtensionRunner, ExtensionToolContext } from "@earendil-works/pi-coding-agent";

// Preserve lazy/guarded host properties (executeTool is non-enumerable in 0.99).
// Local core definitions need a real tool context too, even on middleware paths.
export const executionToolContext = (
  context: ExtensionContext,
  runner: ExtensionRunner | undefined,
  toolCallId: string,
  signal?: AbortSignal,
): ExtensionToolContext => {
  if (runner?.createToolContext) return runner.createToolContext(toolCallId, signal);
  if ("executeTool" in context && "tools" in context) return context as ExtensionToolContext;
  return Object.defineProperties(Object.create(context), {
    tools: { get: () => [] },
    executeTool: { value: async () => { throw new Error("Nested tool calls require a Pi tool execution context"); } },
  });
};
