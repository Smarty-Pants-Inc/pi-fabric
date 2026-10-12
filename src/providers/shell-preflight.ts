import type { ExtensionRunner } from "@earendil-works/pi-coding-agent";
import { runAbortable } from "../async-settlement.js";
import type { CapturedToolCatalog } from "../capture/catalog.js";
import type { FabricPowerShellPolicy } from "../config.js";
import type { PiShellToolName } from "../core/pi-tools.js";
import type { FabricInvocationContext } from "../protocol.js";

// The per-call cwd argument key; equal to PI_BASH_CWD_KEY in pi-bash-cwd.ts,
// which is not imported here so this module's import graph stays light.
const SHELL_CWD_KEY = "cwd";

const shellQuote = (value: string): string => "'" + value.replaceAll("'", "'\\''") + "'";

/** The refusal for any tool_call hook change to a judged shell call (smarty-dev#8606). */
export const SHELL_REWRITE_REFUSED = "a tool_call hook rewrote a shell command; Fabric runs only the command the guards judged";

// Equal to the key in guards/actor-bash-timeout.ts, not imported so this module's
// import graph stays light. Fabric's own run-defaults hook (total timeout cap,
// idle watchdog) sees this holder on the copy and, instead of rewriting it,
// leaves the transform here; it is applied to the judged pi.bash arguments.
const RUN_BASH_DEFAULTS = Symbol.for("pi-fabric.bash-defaults.deferred");
type DeferredRunDefaults = { apply?: (target: Record<string, unknown>) => void };

type ToolCallRunner = Pick<ExtensionRunner, "emitToolCall">;

/**
 * smarty-dev#9826: no guard judges PowerShell grammar. The fleet's command
 * guards are bash-only, and showing them PowerShell source as a bash call is
 * not a safety mechanism (PowerShell-only spellings parse differently). So
 * pi.powershell and every captured or extension powershell override are
 * refused unless the host Fabric profile opts in, accepting that gap.
 */
export const POWERSHELL_REFUSED_MESSAGE =
  'pi.powershell is refused (smarty-dev#9826): no Fabric guard judges PowerShell grammar, and bash command guards cannot judge PowerShell source, ' +
  'so the built-in powershell tool and every extension powershell override stay off. ' +
  'To accept that gap, the host Fabric profile must set executor.powershell to "allow" in its fabric.json.';

/** Throws the #9826 refusal unless the shell is not PowerShell or the profile opted in. */
export const assertShellToolPermitted = (name: string, powershell: FabricPowerShellPolicy | undefined): void => {
  if (name === "powershell" && powershell !== "allow") throw new Error(POWERSHELL_REFUSED_MESSAGE);
};

/** The refusal for any tool_call hook attempt to change a shell call's tool identity (smarty-dev#8606). */
export const SHELL_IDENTITY_REFUSED =
  "a tool_call hook tried to change the shell call's tool identity (type, toolName or toolCallId); every guard must judge the call as the tool that runs it";

// The event keys that name the call. Pi's runner hands the same event object to
// every tool_call handler in turn (ExtensionRunner.emitToolCall), so an earlier
// hook that renamed the tool (say toolName "bash" to "read") would make a later
// bash guard skip the call while Fabric still ran the shell command, and a hook
// that renamed and later restored it would leave no trace in the final value.
const IDENTITY_KEYS = ["type", "toolName", "toolCallId"] as const;
const IDENTITY_WRITE_ERROR = /\b(?:read only property|redefine property|delete property)\b:? '?(?:type|toolName|toolCallId)\b/;

/**
 * Emit one tool_call event; reports whether any hook replaced event.input or
 * changed any of its values (compared by value, nested objects included).
 *
 * The event's identity (type, toolName, toolCallId) is bound read-only and
 * non-configurable for the whole dispatch, so no hook can change what a later
 * hook sees: an assignment throws in strict mode (refusing the call) and is
 * ignored otherwise, and redefining or deleting the key throws. input stays
 * writable; its changes are detected and refused by the caller.
 */
const emitToolCall = async (
  runner: ToolCallRunner,
  signal: AbortSignal | undefined,
  toolName: string,
  toolCallId: string,
  input: Record<string, unknown>,
  blocked: string,
) => {
  const judged = JSON.stringify(input);
  const identity = { type: "tool_call" as const, toolName, toolCallId };
  const event = { input } as { readonly type: "tool_call"; readonly toolName: string; readonly toolCallId: string; input: Record<string, unknown> };
  for (const key of IDENTITY_KEYS) {
    Object.defineProperty(event, key, { value: identity[key], writable: false, configurable: false, enumerable: true });
  }
  let preflight: Awaited<ReturnType<ToolCallRunner["emitToolCall"]>>;
  try {
    preflight = await runAbortable(signal, () => runner.emitToolCall(event));
  } catch (error) {
    // A strict-mode hook's write to a bound key throws out of Pi's dispatch;
    // the call is refused either way, this only names the reason.
    if (error instanceof TypeError && IDENTITY_WRITE_ERROR.test(error.message)) {
      throw new Error(`${blocked}: ${SHELL_IDENTITY_REFUSED}`, { cause: error });
    }
    throw error;
  }
  // Defense in depth: the binding above makes this unreachable, but a shell
  // call whose identity is not the one every hook was shown never runs.
  for (const key of IDENTITY_KEYS) {
    const descriptor = Object.getOwnPropertyDescriptor(event, key);
    if (!descriptor || descriptor.value !== identity[key] || descriptor.writable || descriptor.configurable) {
      throw new Error(`${blocked}: ${SHELL_IDENTITY_REFUSED}`);
    }
  }
  return { preflight, rewritten: event.input !== input || JSON.stringify(input) !== judged };
};

export interface ShellCallPreflight {
  /** The shell tool that will execute the call. */
  name: PiShellToolName;
  /** The caller's arguments. Hooks never receive this object, only copies. */
  args: Record<string, unknown>;
  /** The cwd guards judge commands in; any other cwd is shown as a leading cd. */
  sessionCwd: string;
  toolCallId: string;
  runner: ToolCallRunner;
  signal?: AbortSignal | undefined;
  /** Refusal text when a blocking hook gives no reason. */
  blocked?: string;
  /** Runs before a hook's block is thrown. */
  onBlock?: () => void;
  /** The profile's executor.powershell; absent means refused (smarty-dev#9826). */
  powershell?: FabricPowerShellPolicy | undefined;
}

/**
 * The single tool_call preflight for every shell call Fabric runs: pi.bash,
 * pi.powershell, a captured extension override of either, and state commands
 * (smarty-dev#8606). Resolves to the arguments to execute; throws a hook's
 * refusal.
 *
 * Hooks see a copy; a non-session cwd is shown as the leading cd a native call
 * needs. The call runs only exactly as the guards judged it: a hook that
 * replaces the input or changes any value of it (command, cwd, env, timeout,
 * shell, background, monitor or any other argument) refuses the call. With
 * several hooks, a later one could otherwise turn a command an earlier guard
 * allowed into one it refuses. The only post-judgment change is Fabric's own
 * run defaults for pi.bash (total timeout cap, idle watchdog), handed back by
 * its hook through the RUN_BASH_DEFAULTS holder instead of a rewrite.
 *
 * PowerShell is refused before any hook runs unless the profile opted in
 * (call.powershell === "allow", smarty-dev#9826). The bash preflight below is
 * not what makes it safe: a bash guard reads PowerShell source by bash rules.
 * With the opt-in, command guards (which judge only toolName "bash") are
 * first shown the call as a bash tool_call (behind the same leading cd, with
 * input.shell naming the real shell), then hooks see it under its real name.
 */
export const preflightShellCall = async (call: ShellCallPreflight): Promise<Record<string, unknown>> => {
  const { name, args, sessionCwd, toolCallId, runner, signal } = call;
  assertShellToolPermitted(name, call.powershell);
  const blocked = call.blocked || `Pi tool ${name} was blocked`;
  const judge = async (toolName: string, id: string, input: Record<string, unknown>): Promise<void> => {
    const { preflight, rewritten } = await emitToolCall(runner, signal, toolName, id, input, blocked);
    if (preflight?.block) {
      call.onBlock?.();
      throw new Error(preflight.reason || blocked);
    }
    if (rewritten) throw new Error(`${blocked}: ${SHELL_REWRITE_REFUSED}`);
  };
  const command = args.command;
  if (name !== "bash" && typeof command !== "string") {
    throw new Error(`Pi tool ${name} was blocked: it has no command text for the bash guard to judge`);
  }
  const cwd = args[SHELL_CWD_KEY];
  const preface = typeof cwd === "string" && typeof command === "string" && cwd !== sessionCwd
    ? `cd ${shellQuote(cwd)} && ` : undefined;
  const view: Record<string, unknown> = { ...args };
  if (preface) view.command = preface + (command as string);
  if (name !== "bash") view.shell = name;
  const defaults: DeferredRunDefaults = {};
  Object.defineProperty(view, RUN_BASH_DEFAULTS, { value: defaults });
  await judge("bash", name === "bash" ? toolCallId : `${toolCallId}:bash-guard`, view);
  // Guards for the real tool name still see the call, but nothing may change it.
  if (name !== "bash") await judge(name, toolCallId, { ...args });
  const input: Record<string, unknown> = { ...args };
  if (name === "bash") defaults.apply?.(input);
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
 * so a hook may approve or refuse one but never rewrite it: like every shell
 * preflight, any change to the judged input is a refusal, and the run defaults
 * that pi.bash gets after judgment are not applied to a state command.
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
    // guard judges them there.
    await preflightShellCall({
      name: "bash",
      args: { command },
      sessionCwd: context.cwd,
      toolCallId: `${context.nestedToolCallId}:${source}:${index}`,
      runner,
      signal: context.signal,
      blocked: `${source} command was blocked`,
    });
  };
