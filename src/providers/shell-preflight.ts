import { runAbortable } from "../async-settlement.js";
import type { CapturedToolCatalog } from "../capture/catalog.js";
import type { FabricInvocationContext } from "../protocol.js";

/**
 * Emits the bash tool_call preflight for a shell command that a Fabric
 * provider runs itself rather than through pi.bash, so guard extensions see it
 * exactly as they see a native bash call. Returns the command to run (a hook
 * may rewrite it); throws the hook's refusal. Without an observed extension
 * runner nothing could refuse the command, so it fails closed (smarty-dev#8606).
 */
export type ShellCommandPreflight = (
  command: string,
  context: FabricInvocationContext,
  source: string,
  index: number,
) => Promise<string>;

export const createShellCommandPreflight = (catalog: CapturedToolCatalog): ShellCommandPreflight =>
  async (command, context, source, index) => {
    const runner = catalog.runner;
    if (!runner) {
      throw new Error(`${source} is unavailable until extension tool hooks initialize; retry once tools are ready`);
    }
    const input: { command: string } = { command };
    const preflight = await runAbortable(context.signal, () => runner.emitToolCall({
      type: "tool_call",
      toolName: "bash",
      toolCallId: `${context.nestedToolCallId}:${source}:${index}`,
      input,
    }));
    if (preflight?.block) throw new Error(preflight.reason || `${source} command was blocked`);
    if (typeof input.command !== "string") throw new Error(`${source} command was rewritten to a non-string by a tool_call hook`);
    return input.command;
  };
