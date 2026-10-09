import { randomBytes } from "node:crypto";

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
/** Exit code of an idle-killed command, as timeout(1). */
export const BASH_IDLE_EXIT_CODE = 124;
/** First line of a wrapped command; also stops a second hook from wrapping it again. */
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

const shellQuote = (value: string): string => `'${value.replaceAll("'", `'\\''`)}'`;

// Runs as `node -e`, argv tail [idleSeconds, shell], command on stdin. The command runs in the same
// shell and process group (so Pi's own abort and total timeout still kill it). After N seconds without
// output it SIGKILLs the command's descendants (setsid children too, while their parent chain is
// alive) and, when it leads its group, the rest of the group (orphaned background jobs); then it
// reports and exits 124. After the shell exits it keeps Pi's 100 ms quiet-pipe grace.
const WATCHDOG = `
const fs=require("fs"),cp=require("child_process"),os=require("os");
const[s,sh]=process.argv.slice(-2),n=Number(s),me=process.pid;
const c=cp.spawn(sh,["-c",fs.readFileSync(0,"utf8")],{stdio:["ignore","pipe","pipe"]});
let t,g,code=1,exited=false,idle=false,done=false;
const finish=()=>{if(done)return;done=true;clearTimeout(t);clearTimeout(g);let p=2;const e=()=>--p||process.exit(idle?${BASH_IDLE_EXIT_CODE}:code);process.stdout.write("",e);process.stderr.write("",e)};
const sweep=()=>{let rows=[];try{rows=cp.execFileSync("ps",["-A","-o","pid=,ppid=,pgid="],{encoding:"utf8"}).trim().split("\\n").map(l=>l.trim().split(/\\s+/).map(Number))}catch{}
const kill=new Set([c.pid]);for(let grew=true;grew;){grew=false;for(const[p,pp]of rows)if(kill.has(pp)&&!kill.has(p)){kill.add(p);grew=true}}
if(rows.some(([p,,pg])=>p===me&&pg===me))for(const[p,,pg]of rows)if(pg===me&&p!==me)kill.add(p);
for(const p of kill)try{process.kill(p,"SIGKILL")}catch{}};
const expire=()=>{idle=true;process.stderr.write("\\n[pi-fabric] bash idle timeout: no output for "+n+" s; killed; rerun with a bounded range or a command that prints progress\\n");sweep();sweep();g=setTimeout(finish,1000)};
const arm=()=>{clearTimeout(t);if(!exited&&!idle)t=setTimeout(expire,n*1000)};
const forward=w=>d=>{w.write(d);if(exited){clearTimeout(g);g=setTimeout(finish,100)}else arm()};
c.stdout.on("data",forward(process.stdout));c.stderr.on("data",forward(process.stderr));
c.on("error",e=>{process.stderr.write(String(e)+"\\n");code=127;finish()});
c.on("exit",(x,sig)=>{exited=true;clearTimeout(t);code=x??128+(os.constants.signals[sig]||0);clearTimeout(g);g=setTimeout(finish,100)});
c.on("close",finish);
arm();
`.trim();

/** The command wrapped in the idle watchdog. The original runs verbatim from a quoted heredoc. */
export const idleWatchdogCommand = (command: string, idleSeconds: number, nodePath = process.execPath): string => {
  const delimiter = `PI_FABRIC_IDLE_${randomBytes(12).toString("hex")}`;
  return `${BASH_IDLE_MARKER}\nexec ${shellQuote(nodePath)} -e ${shellQuote(WATCHDOG)} ${idleSeconds} "\${BASH:-$0}" <<'${delimiter}'\n${command}\n${delimiter}\n`;
};

/**
 * Apply both run defaults to a bash tool_call input in place: the total cap when the call has no
 * timeout, and the idle watchdog when the call blocks the agent (not on Windows). fabric_exec's
 * pi.bash `background`/`monitor` jobs detach at once, so they may be silent.
 */
export const applyRunBashDefaults = (
  env: Env, input: { command?: unknown; timeout?: unknown; background?: unknown; monitor?: unknown },
  platform: NodeJS.Platform = process.platform,
): void => {
  const idle = bashIdleSeconds(env);
  const detached = input.background === true || input.monitor !== undefined;
  if (idle !== undefined && !detached && platform !== "win32" && typeof input.command === "string" && !input.command.startsWith(BASH_IDLE_MARKER)) {
    input.command = idleWatchdogCommand(input.command, idle);
  }
  const total = actorBashTimeout(env, input.timeout);
  if (total !== undefined) input.timeout = total;
};
