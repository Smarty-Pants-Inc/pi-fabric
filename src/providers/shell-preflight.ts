import { runAbortable } from "../async-settlement.js";
import type { CapturedToolCatalog } from "../capture/catalog.js";
import type { FabricInvocationContext } from "../protocol.js";

/**
 * Emits the bash tool_call preflight for a shell command that a Fabric
 * provider runs itself rather than through pi.bash, so guard extensions see it
 * exactly as they see a native bash call. Resolves when the command may run as
 * declared; throws the hook's refusal. Without an observed extension runner
 * nothing could refuse the command, so it fails closed (smarty-dev#8606).
 *
 * State commands are operator evidence whose text is recorded and digested,
 * so a hook may approve or refuse one but never rewrite it: a rewrite would
 * run a command other than the one the goal or certificate names. Any change
 * to the judged input is a refusal.
 */
export type ShellCommandPreflight = (
  command: string,
  context: FabricInvocationContext,
  source: string,
  index: number,
) => Promise<void>;

export const createShellCommandPreflight = (catalog: CapturedToolCatalog): ShellCommandPreflight =>
  async (command, context, source, index) => {
    const runner = catalog.runner;
    if (!runner) {
      throw new Error(`${source} is unavailable until extension tool hooks initialize; retry once tools are ready`);
    }
    const input: Record<string, unknown> = { command };
    const judged = JSON.stringify(input);
    const event = {
      type: "tool_call" as const,
      toolName: "bash" as string,
      toolCallId: `${context.nestedToolCallId}:${source}:${index}`,
      input,
    };
    const preflight = await runAbortable(context.signal, () => runner.emitToolCall(event));
    if (preflight?.block) throw new Error(preflight.reason || `${source} command was blocked`);
    if (event.input !== input || JSON.stringify(input) !== judged) {
      throw new Error(`${source} command was blocked: a tool_call hook rewrote it, and state commands run only as declared because the goal or certificate records them`);
    }
  };
