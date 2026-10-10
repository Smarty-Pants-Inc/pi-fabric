import { spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { existsSync } from "node:fs";
import path from "node:path";
import { loadedFabricRoot } from "../core/agent-dir.js";

// smarty-dev#2184: an actor run's bash call without a timeout could hang the run (and the actor's removal)
// for good. Actor runs (PI_FABRIC_ACTOR_ID set) get a default per-command timeout; an actor's
// bashTimeoutSeconds (exported as PI_FABRIC_ACTOR_BASH_TIMEOUT_S) overrides it, 0 turns it off.
//
// smarty-dev#6137: task agents hung for 7 minutes on silent commands (`git log --all -S`), under the
// 600 s cap. The worker now exports PI_FABRIC_BASH_IDLE_S into every Pi run it launches (task agents
// and actors, never Mains); its presence also gives task agents the total cap. A blocking bash call
// runs under a small idle watchdog that kills the command after that many seconds with no
// stdout/stderr output. An explicit per-call timeout replaces only the total cap: GPT-6.1 Sol passes
// `timeout` on its own (the #6137 replay sent timeout 0, then 1000, for the same silent git log), so
// a timeout opt-out would miss the real case. The spawner opts out with bashIdleSeconds: 0.
// ponytail: Pi 0.87.1's bash tool has only a total timeout (createLocalShellOperations sets one
// setTimeout) and gives extensions no output hook they can act on (onUpdate feeds the renderer; the
// tool_execution_update event cannot kill). Rewriting the command in tool_call is the smallest hook.

export const DEFAULT_ACTOR_BASH_TIMEOUT_S = 600;
/** Largest whole-second timeout within Pi's signed 32-bit millisecond timer limit. */
export const MAX_ACTOR_BASH_TIMEOUT_S = 2_147_483;
/** Default seconds without output before a Fabric run's bash call is killed (smarty-dev#6137). */
export const DEFAULT_BASH_IDLE_S = 180;
export { BASH_IDLE_EXIT_CODE, BASH_IDLE_TERM_GRACE_S } from "./bash-idle-policy.js";
/** Diagnostic first line only: command text is never evidence that a call was wrapped. */
export const BASH_IDLE_MARKER = "# pi-fabric bash idle watchdog (smarty-dev#6137)";

type Env = Readonly<Record<string, string | undefined>>;

/** A run Fabric's worker launched (task agent or actor), not a Main. */
const fabricRun = (env: Env): boolean => Boolean(env.PI_FABRIC_ACTOR_ID) || env.PI_FABRIC_BASH_IDLE_S !== undefined;

const seconds = (raw: string | undefined, fallback: number): number | undefined => {
  const value = raw ? Number(raw) : fallback;
  if (value === 0) return undefined;
  return Number.isInteger(value) && value > 0 && value <= MAX_ACTOR_BASH_TIMEOUT_S ? value : fallback;
};

/** The timeout (seconds) to set on a bash call that has none, or undefined to leave it as is. */
export const actorBashTimeout = (env: Env, timeout: unknown): number | undefined =>
  !fabricRun(env) || timeout !== undefined ? undefined : seconds(env.PI_FABRIC_ACTOR_BASH_TIMEOUT_S, DEFAULT_ACTOR_BASH_TIMEOUT_S);

/** Idle (no-output) limit in seconds for this run's bash calls, or undefined for none. */
export const bashIdleSeconds = (env: Env): number | undefined =>
  fabricRun(env) ? seconds(env.PI_FABRIC_BASH_IDLE_S, DEFAULT_BASH_IDLE_S) : undefined;

// The standalone hook and Fabric bundle have separate module instances. Share a host-only nonce,
// and put it in non-enumerable symbol metadata on the actual tool-call args. JSON input cannot
// supply it, and copying/serializing a wrapped command must not exempt a new untrusted call.
const WRAPPED = Symbol.for("pi-fabric.bash-idle.wrapped");
const WRAP_STATE = Symbol.for("pi-fabric.bash-idle.wrap-state");
const host = globalThis as typeof globalThis & { [WRAP_STATE]?: { nonce: string } };
const wrapNonce = (host[WRAP_STATE] ??= { nonce: randomBytes(24).toString("hex") }).nonce;
type BashInput = { command?: unknown; timeout?: unknown; background?: unknown; monitor?: unknown; [WRAPPED]?: string };

const shellQuote = (value: string): string => `'${value.replaceAll("'", `'\\''`)}'`;

// Resolve at actual first wrap, not registration. Package-root lookup works from source,
// dist/index.js, dist/chunks/* and the separately built dist/guards/actor-bash-hook.js.
let watchdogPath: string | undefined;
const idleWatchdogPath = (): string => {
  if (watchdogPath) return watchdogPath;
  const root = loadedFabricRoot(import.meta.url);
  if (!root) throw new Error("Cannot locate the Fabric bash idle watchdog package");
  return watchdogPath = path.join(root, "dist", "bash-idle-watchdog.js");
};

const idleShellPath = (): string => {
  if (process.env.BASH && path.isAbsolute(process.env.BASH)) return process.env.BASH;
  // Mirror Pi's POSIX getShellConfig: /bin/bash, `which bash`, then sh on PATH.
  // Importing the host barrel here also bundles it into the standalone worker.
  if (existsSync("/bin/bash")) return "/bin/bash";
  let shell = "sh";
  try {
    const found = spawnSync("which", ["bash"], { encoding: "utf8", timeout: 5000 });
    const first = found.status === 0 ? found.stdout?.trim().split(/\r?\n/)[0] : undefined;
    if (first) shell = first;
  } catch { /* Pi falls back to sh if which is unavailable */ }
  if (path.isAbsolute(shell)) return shell;
  for (const directory of (process.env.PATH ?? "").split(path.delimiter)) {
    const candidate = path.resolve(directory, shell);
    if (existsSync(candidate)) return candidate;
  }
  throw new Error(`Cannot resolve Pi's bash shell to an absolute path: ${shell}`);
};

/** The command wrapped in the idle watchdog. The original runs verbatim from a quoted heredoc. */
export const idleWatchdogCommand = (command: string, idleSeconds: number, nodePath = process.execPath): string => {
  const delimiter = `PI_FABRIC_IDLE_${randomBytes(12).toString("hex")}`;
  return `${BASH_IDLE_MARKER}\nexec ${shellQuote(nodePath)} ${shellQuote(idleWatchdogPath())} ${idleSeconds} ${shellQuote(idleShellPath())} <<'${delimiter}'\n${command}\n${delimiter}\n`;
};

/**
 * Apply both run defaults to a bash tool_call input in place: the total cap when the call has no
 * timeout, and the idle watchdog when the call blocks the agent (not on Windows). fabric_exec's
 * pi.bash `background`/`monitor` jobs detach at once, so they may be silent.
 */
export const applyRunBashDefaults = (
  env: Env, input: BashInput,
  platform: NodeJS.Platform = process.platform,
): void => {
  const total = actorBashTimeout(env, input.timeout);
  if (total !== undefined) input.timeout = total;
  // ponytail: Windows has no POSIX process groups/setsid or group TERM/KILL signaling.
  // Keep the total cap above, with no idle watchdog or command rewrite on win32.
  // A Windows idle-limit implementation remains a platform limit tracked by smarty-dev#6137.
  if (platform === "win32") return;
  const idle = bashIdleSeconds(env);
  const detached = input.background === true || input.monitor !== undefined;
  if (idle !== undefined && !detached && typeof input.command === "string" && input[WRAPPED] !== wrapNonce) {
    input.command = idleWatchdogCommand(input.command, idle);
    Object.defineProperty(input, WRAPPED, { value: wrapNonce });
  }
};
