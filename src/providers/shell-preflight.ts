import type { ExtensionRunner } from "@earendil-works/pi-coding-agent";
import { runAbortable } from "../async-settlement.js";
import type { CapturedToolCatalog } from "../capture/catalog.js";
import type { PiShellToolName } from "../core/pi-tools.js";
import type { FabricInvocationContext } from "../protocol.js";

// The per-call cwd argument key; equal to PI_BASH_CWD_KEY in pi-bash-cwd.ts,
// which is not imported here so this module's import graph stays light.
const SHELL_CWD_KEY = "cwd";

const shellQuote = (value: string): string => "'" + value.replaceAll("'", "'\\''") + "'";

type ToolCallRunner = Pick<ExtensionRunner, "emitToolCall">;

/** Emit one tool_call event; reports whether a hook replaced event.input. */
const emitToolCall = async (
  runner: ToolCallRunner,
  signal: AbortSignal | undefined,
  toolName: string,
  toolCallId: string,
  input: Record<string, unknown>,
) => {
  const event = { type: "tool_call" as const, toolName, toolCallId, input };
  const preflight = await runAbortable(signal, () => runner.emitToolCall(event));
  return { preflight, replaced: event.input !== input };
};

export interface ShellCallPreflight {
  /** The shell tool that will execute the call. */
  name: PiShellToolName;
  /** The caller's arguments. Hooks never receive this object, only copies. */
  args: Record<string, unknown>;
  /** The cwd guards judge commands in; any other cwd is shown as a leading cd. */
  sessionCwd: string;
  /** Where the call runs without a cwd argument (defaults to sessionCwd). */
  defaultCwd?: unknown;
  toolCallId: string;
  runner: ToolCallRunner;
  signal?: AbortSignal | undefined;
  /** Refusal text when a blocking hook gives no reason. */
  blocked?: string;
  /** Runs before a hook's block is thrown. */
  onBlock?: () => void;
  /** bash only: refuse any change to the judged input with this text (no rewrites at all). */
  frozen?: string;
}

/**
 * The single tool_call preflight for every shell call Fabric runs: pi.bash,
 * pi.powershell, and a captured extension override of either (smarty-dev#8606).
 * Resolves to the arguments to execute; throws a hook's refusal.
 *
 * bash: the hook sees a copy of the args, with a non-session cwd shown as the
 * leading cd a native call needs. The effective cwd (args.cwd, or defaultCwd
 * when absent) is snapshotted before the hook runs; a hook that adds, changes
 * or otherwise moves it is refused, as is one that drops the judged cd. A
 * command-only rewrite is returned for execution.
 *
 * Other shells: command guards judge only toolName "bash", so the call is
 * first shown to them as a bash tool_call (behind the same leading cd, with
 * input.shell naming the real shell), then to hooks under its real name. A
 * bash rewrite cannot carry over to another shell's syntax, so any change to
 * either judged input is refused; the call runs exactly as judged.
 */
export const preflightShellCall = async (call: ShellCallPreflight): Promise<Record<string, unknown>> => {
  const { name, args, sessionCwd, toolCallId, runner, signal } = call;
  const refuse = (reason: string | undefined): Error => {
    call.onBlock?.();
    return new Error(reason || call.blocked || `Pi tool ${name} was blocked`);
  };
  const command = args.command;
  const cwd = args[SHELL_CWD_KEY];
  const preface = typeof cwd === "string" && typeof command === "string" && cwd !== sessionCwd
    ? `cd ${shellQuote(cwd)} && ` : undefined;

  if (name !== "bash") {
    if (typeof command !== "string") {
      throw new Error(`Pi tool ${name} was blocked: it has no command text for the bash guard to judge`);
    }
    const view: Record<string, unknown> = { ...args, command: (preface ?? "") + command, shell: name };
    const judgedView = JSON.stringify(view);
    const guard = await emitToolCall(runner, signal, "bash", `${toolCallId}:bash-guard`, view);
    if (guard.preflight?.block) throw refuse(guard.preflight.reason);
    if (guard.replaced || JSON.stringify(view) !== judgedView) {
      throw new Error(`Pi tool ${name} was blocked: a tool_call hook rewrote the ${name} command it judged as bash; it runs only exactly as judged`);
    }
    // Guards for the real tool name still see the call, but nothing may
    // change it after the bash view judged it.
    const input: Record<string, unknown> = { ...args };
    const judged = JSON.stringify(input);
    const named = await emitToolCall(runner, signal, name, toolCallId, input);
    if (named.preflight?.block) throw refuse(named.preflight.reason);
    if (named.replaced || JSON.stringify(input) !== judged) {
      throw new Error(`Pi tool ${name} was blocked: a tool_call hook rewrote the command after the bash guard judged it`);
    }
    return input;
  }

  const effectiveCwd = (value: Record<string, unknown>): unknown => {
    const requested = value[SHELL_CWD_KEY];
    return requested !== undefined ? requested : (call.defaultCwd ?? sessionCwd);
  };
  const judgedCwd = effectiveCwd(args);
  const input: Record<string, unknown> = preface ? { ...args, command: preface + (command as string) } : { ...args };
  const judged = JSON.stringify(input);
  const { preflight, replaced } = await emitToolCall(runner, signal, name, toolCallId, input);
  if (preflight?.block) throw refuse(preflight.reason);
  if (call.frozen !== undefined && (replaced || JSON.stringify(input) !== judged)) throw new Error(call.frozen);
  // Compared by value: a hook that rewrites the cwd into any other form is
  // treated as moving execution.
  if (effectiveCwd(input) !== judgedCwd) {
    throw new Error(`Pi tool ${name} was blocked: a tool_call hook moved the cwd its command was judged in; a hook may not move execution`);
  }
  if (preface) {
    // Carry hook rewrites back, but only through the cd it judged.
    if (typeof input.command !== "string" || !input.command.startsWith(preface) || input[SHELL_CWD_KEY] !== cwd) {
      throw new Error(`Pi tool ${name} was blocked: a tool_call hook rewrote the cwd it judged; run the command without cwd`);
    }
    input.command = input.command.slice(preface.length);
  }
  return input;
};

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
    // State commands run in the invocation cwd with no cwd argument, so the
    // guard judges them there; the shared helper still snapshots that cwd.
    await preflightShellCall({
      name: "bash",
      args: { command },
      sessionCwd: context.cwd,
      toolCallId: `${context.nestedToolCallId}:${source}:${index}`,
      runner,
      signal: context.signal,
      blocked: `${source} command was blocked`,
      frozen: `${source} command was blocked: a tool_call hook rewrote it, and state commands run only as declared because the goal or certificate records them`,
    });
  };
