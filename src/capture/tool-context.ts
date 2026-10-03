import type { ExtensionContext, ExtensionRunner, ToolDefinition } from "@earendil-works/pi-coding-agent";

// Indexed from the host execute signature so this stays source-compatible with
// older Pi, where tools received only ExtensionContext.
type ToolExecutionContext = Parameters<ToolDefinition<any, any, any>["execute"]>[4];

/** Use the live host's nested-call context; never implement a second tool pipeline. */
export const createCapturedToolContext = (
  runner: ExtensionRunner | undefined,
  toolCallId: string,
  signal: AbortSignal | undefined,
  fallback?: ExtensionContext,
  cwd?: string,
): ToolExecutionContext => {
  const host = runner as (ExtensionRunner & {
    createToolContext?: (id: string, signal: AbortSignal | undefined) => ToolExecutionContext;
  }) | undefined;
  const context = typeof host?.createToolContext === "function"
    ? host.createToolContext(toolCallId, signal)
    : fallback ?? runner?.createContext();
  if (!context) throw new Error("Captured tool execution needs a host context");
  // Object.create preserves Pi's guarded getters and non-enumerable nested
  // methods. Object spread freezes session-bound getters and drops those APIs.
  const result = Object.create(context) as ToolExecutionContext;
  if (!("executeTool" in context)) {
    Object.defineProperties(result, {
      tools: { value: [] },
      executeTool: { value: async () => { throw new Error("Nested tool calls require Pi 0.99+ host support"); } },
    });
  }
  if (cwd !== undefined) Object.defineProperty(result, "cwd", { value: cwd, enumerable: true });
  return result;
};
