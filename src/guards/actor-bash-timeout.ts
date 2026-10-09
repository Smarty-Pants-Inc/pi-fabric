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
/** Diagnostic first line only: command text is never evidence that a call was wrapped. */
export const BASH_IDLE_MARKER = "# pi-fabric bash idle watchdog (smarty-dev#6137)";
/** Bounded TERM-to-KILL grace; output/early shell exit cannot cancel escalation. */
export const BASH_IDLE_TERM_GRACE_S = 5;

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

// This detached execution envelope is the command's process-group leader. IPC custody ensures
// Pi killing the outer watchdog on total timeout/abort also kills this newly detached group.
// Ignore TERM in the envelope so it retains custody while the command handles TERM during grace.
const GROUP_RUNNER = `
const cp=require("child_process"),os=require("os");
process.on("SIGTERM",()=>{});
process.on("disconnect",()=>{try{process.kill(-process.pid,"SIGKILL")}catch{process.exit(1)}});
const[sh,command]=process.argv.slice(-2);
const c=cp.spawn(sh,["-c",command],{stdio:["ignore","inherit","inherit"]});
c.on("error",e=>{process.stderr.write(String(e)+"\\n");process.exit(127)});
c.on("exit",(code,sig)=>process.exit(code??128+(os.constants.signals[sig]||0)));
`.trim();

// Runs as `node -e`, argv tail [idleSeconds, shell], command on stdin. A detached group makes
// reparented background jobs killable even when this watchdog isn't its own group leader.
// Linux /proc ppid traversal is a second net for setsid children, with start times retained
// across TERM/reparenting to avoid signaling reused PIDs. No ps executable is required.
// A double-forked setsid daemon that leaves the group before discovery is not covered (#7934).
// No per-command delegated cgroup-v2 launch is reachable from this lightweight tool_call hook.
const WATCHDOG = `
const fs=require("fs"),cp=require("child_process"),os=require("os");
const[s,sh]=process.argv.slice(-2),n=Number(s);
const c=cp.spawn(process.execPath,["-e",${JSON.stringify(GROUP_RUNNER)},sh,fs.readFileSync(0,"utf8")],{detached:true,stdio:["ignore","pipe","pipe","ipc"]});
let t,g,code=1,exited=false,idle=false,done=false;
const tracked=new Map();
const finish=()=>{if(done)return;done=true;clearTimeout(t);clearTimeout(g);let p=2;const e=()=>--p||process.exit(idle?${BASH_IDLE_EXIT_CODE}:code);process.stdout.write("",e);process.stderr.write("",e)};
const rows=()=>{const result=new Map();try{for(const name of fs.readdirSync("/proc")){if(!/^\\d+$/.test(name))continue;try{const stat=fs.readFileSync("/proc/"+name+"/stat","utf8"),a=stat.slice(stat.lastIndexOf(")")+2).split(" ");result.set(Number(name),{pp:Number(a[1]),pg:Number(a[2]),start:a[19]})}catch{}}}catch{}return result};
const sweep=sig=>{const all=rows(),owned=new Set([c.pid]);for(const[p,start]of tracked)if(all.get(p)?.start===start)owned.add(p);
for(let grew=true;grew;){grew=false;for(const[p,r]of all)if((owned.has(r.pp)||r.pg===c.pid)&&!owned.has(p)){owned.add(p);grew=true}}
for(const p of owned){const r=all.get(p);if(r)tracked.set(p,r.start)}
try{process.kill(-c.pid,sig)}catch{}
for(const[p,start]of tracked){const r=all.get(p);if(r?.start===start&&r.pg!==c.pid)try{process.kill(p,sig)}catch{}}};
const expire=()=>{idle=true;process.stderr.write("\\n[pi-fabric] bash idle timeout: no output for "+n+" s; killed; rerun with a bounded range or a command that prints progress\\n");sweep("SIGTERM");g=setTimeout(()=>{sweep("SIGKILL");finish()},${BASH_IDLE_TERM_GRACE_S}*1000)};
const arm=()=>{clearTimeout(t);if(!exited&&!idle)t=setTimeout(expire,n*1000)};
const forward=w=>d=>{w.write(d);if(idle)return;if(exited){clearTimeout(g);g=setTimeout(finish,100)}else arm()};
c.stdout.on("data",forward(process.stdout));c.stderr.on("data",forward(process.stderr));
c.on("error",e=>{process.stderr.write(String(e)+"\\n");code=127;if(!idle)finish()});
c.on("exit",(x,sig)=>{exited=true;clearTimeout(t);code=x??128+(os.constants.signals[sig]||0);if(!idle){clearTimeout(g);g=setTimeout(finish,100)}});
c.on("close",()=>{if(!idle)finish()});
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
  env: Env, input: BashInput,
  platform: NodeJS.Platform = process.platform,
): void => {
  const idle = bashIdleSeconds(env);
  const detached = input.background === true || input.monitor !== undefined;
  if (idle !== undefined && !detached && platform !== "win32" && typeof input.command === "string" && input[WRAPPED] !== wrapNonce) {
    input.command = idleWatchdogCommand(input.command, idle);
    Object.defineProperty(input, WRAPPED, { value: wrapNonce });
  }
  const total = actorBashTimeout(env, input.timeout);
  if (total !== undefined) input.timeout = total;
};
