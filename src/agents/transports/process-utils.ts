import { execFile, spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import type { AgentTransportLaunch } from "../types.js";
import { assertTransportLaunchAllowed } from "./launch-authority.js";

export interface ExecFileResult {
  stdout: string;
  stderr: string;
}

export const executeFile = (
  command: string,
  args: string[],
  options: { cwd?: string; timeoutMs?: number } = {},
): Promise<ExecFileResult> =>
  new Promise((resolve, reject) => {
    execFile(
      command,
      args,
      {
        encoding: "utf8",
        maxBuffer: 10 * 1024 * 1024,
        ...(options.cwd ? { cwd: options.cwd } : {}),
        ...(options.timeoutMs ? { timeout: options.timeoutMs } : {}),
      },
      (error, stdout, stderr) => {
        if (error) {
          Object.assign(error, { stdout, stderr });
          reject(error);
          return;
        }
        resolve({ stdout, stderr });
      },
    );
  });

/**
 * Windows resolves bare names through PATHEXT only: an extensionless file (an
 * npm `sh` shim, for example) is not launchable there. POSIX needs the execute bit.
 */
const executableNames = (command: string, env: NodeJS.ProcessEnv): string[] => {
  if (process.platform !== "win32" || path.extname(command) !== "") return [command];
  const extensions = (env.PATHEXT ?? ".COM;.EXE;.BAT;.CMD")
    .split(";")
    .map((extension) => extension.trim().toLowerCase())
    .filter((extension) => extension !== "");
  return extensions.map((extension) => command + extension);
};

const unquotePathEntry = (entry: string): string => {
  const trimmed = entry.trim();
  return trimmed.length > 1 && trimmed.startsWith('"') && trimmed.endsWith('"')
    ? trimmed.slice(1, -1)
    : trimmed;
};

const isExecutableFile = (candidate: string): boolean => {
  try {
    const stats = fs.statSync(candidate);
    if (!stats.isFile()) return false;
    // Windows has no execute bit; PATHEXT already narrowed the candidate name.
    return process.platform === "win32" || (stats.mode & 0o111) !== 0;
  } catch {
    return false;
  }
};

/**
 * PATH lookup that never shells out: Windows runners reach this code with no
 * `sh` on PATH, and a login shell may rewrite PATH behind the caller's back.
 * Returns an absolute path, so a transport with another environment (Herdr's
 * server, for example) launches the executable the caller selected.
 */
export const findExecutable = (
  command: string,
  env: NodeJS.ProcessEnv = process.env,
  isExecutable: (candidate: string) => boolean = isExecutableFile,
): string | undefined => {
  const names = executableNames(command, env);
  for (const entry of (env.PATH ?? "").split(path.delimiter)) {
    const directory = unquotePathEntry(entry);
    if (directory === "") continue;
    for (const name of names) {
      const candidate = path.resolve(directory, name);
      if (isExecutable(candidate)) return candidate;
    }
  }
  return undefined;
};

export const commandAvailable = async (
  command: string,
  env: NodeJS.ProcessEnv = process.env,
): Promise<boolean> => findExecutable(command, env) !== undefined;

const shellQuote = (value: string): string => `'${value.replaceAll("'", `'"'"'`)}'`;

export const processIsAlive = (pid: number): boolean => {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    // On Windows, EPERM means the process exists but cannot be opened for
    // signaling; only ESRCH (or other errors) mean it is gone.
    return error instanceof Error && "code" in error && (error as NodeJS.ErrnoException).code === "EPERM";
  }
};

const GENERIC_RUNTIME = /^(node|bun)(\.exe)?$/;

export interface ScriptRuntimeOptions {
  execPath?: string;
  env?: NodeJS.ProcessEnv;
  /** Require Node.js specifically; used by the Node-process executor whose
   *  `--eval`/`--input-type=module` flags are Node-only. */
  requireNode?: boolean;
  /** Require Bun specifically; used by the Bun-process executor. */
  requireBun?: boolean;
}

const runtimeOverride = (env: NodeJS.ProcessEnv): string | undefined => {
  const value = env.PI_FABRIC_NODE_BINARY;
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
};

const isGenericRuntime = (execPath: string, requireNode: boolean, requireBun = false): boolean => {
  const name = path.basename(execPath).toLowerCase();
  return GENERIC_RUNTIME.test(name)
    && (!requireNode || name.startsWith("node"))
    && (!requireBun || name.startsWith("bun"));
};

const missingRuntimeError = (execPath: string, requireNode: boolean, requireBun = false): Error => {
  const required = requireNode
    ? "a Node.js runtime"
    : requireBun
      ? "a Bun runtime"
      : "a Node.js or Bun runtime";
  const shape = requireNode ? "(not node)" : requireBun ? "(not bun)" : "(not node/bun)";
  return new Error(
    `Fabric requires ${required} to launch a JavaScript worker, but ` +
      `process.execPath is ${execPath} ${shape} and PI_FABRIC_NODE_BINARY is unset. ` +
      "Install Node.js or Bun, or set PI_FABRIC_NODE_BINARY to the runtime binary.",
  );
};

// Transports launch the worker (a .js module) as `<runtime> worker.js args`.
// Under the new Bun-compiled pi binary, process.execPath is the pi executable,
// not node/bun, so it cannot run an arbitrary script. Resolve a real runtime
// before spawning: reuse process.execPath when it IS node/bun, else fall back
// to PI_FABRIC_NODE_BINARY, then the first node/bun on PATH.
const resolveScriptRuntimeUncached = async (options: ScriptRuntimeOptions = {}): Promise<string> => {
  const execPath = options.execPath ?? process.execPath;
  const env = options.env ?? process.env;
  const requireNode = options.requireNode === true;
  const requireBun = options.requireBun === true;
  if (isGenericRuntime(execPath, requireNode, requireBun)) return execPath;
  const override = runtimeOverride(env);
  if (override) return override;
  for (const candidate of requireNode ? ["node"] : requireBun ? ["bun"] : ["node", "bun"]) {
    const found = findExecutable(candidate);
    if (found) return found;
  }
  throw missingRuntimeError(execPath, requireNode, requireBun);
};

let cachedDefaultRuntime: string | undefined;
export const resolveScriptRuntime = async (options?: ScriptRuntimeOptions): Promise<string> => {
  if (
    options &&
    (options.execPath !== undefined ||
      options.env !== undefined ||
      options.requireNode !== undefined ||
      options.requireBun !== undefined)
  ) {
    return resolveScriptRuntimeUncached(options);
  }
  if (cachedDefaultRuntime) return cachedDefaultRuntime;
  cachedDefaultRuntime = await resolveScriptRuntimeUncached();
  return cachedDefaultRuntime;
};

// Synchronous variant for callers that already run under a real runtime (e.g.
// the worker, which a transport always launches via the resolved runtime). No
// PATH lookup; throws if the current process is the bundled binary with no
// override set.
export const resolveScriptRuntimeSync = (options: ScriptRuntimeOptions = {}): string => {
  const execPath = options.execPath ?? process.execPath;
  const env = options.env ?? process.env;
  const requireNode = options.requireNode === true;
  const requireBun = options.requireBun === true;
  if (isGenericRuntime(execPath, requireNode, requireBun)) return execPath;
  const override = runtimeOverride(env);
  if (override) return override;
  throw missingRuntimeError(execPath, requireNode, requireBun);
};

const typescriptWorker = (workerPath: string): boolean =>
  /.[cm]?tsx?$/i.test(path.extname(workerPath));

const runtimeOptionsForWorker = (
  workerPath: string,
  options?: ScriptRuntimeOptions,
): ScriptRuntimeOptions | undefined => {
  if (!typescriptWorker(workerPath) || options?.requireNode || options?.requireBun) return options;
  return { ...options, requireBun: true };
};

export const scriptSpawnArgs = async (
  workerPath: string,
  workerArguments: readonly string[],
  options?: ScriptRuntimeOptions,
): Promise<string[]> => {
  const runtime = await resolveScriptRuntime(runtimeOptionsForWorker(workerPath, options));
  return [runtime, workerPath, ...workerArguments];
};

export const workerCommand = async (
  workerPath: string,
  workerArguments: string[],
): Promise<string> =>
  (await scriptSpawnArgs(workerPath, workerArguments)).map(shellQuote).join(" ");

type LinuxGroupMember = { pid: number; parent: number; group: number; started: string; state: string };
const linuxGroupMember = (pid: number): LinuxGroupMember | undefined => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
    return { pid, parent: Number(fields[1]), group: Number(fields[2]), started: fields[19]!, state: fields[0]! };
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT" || (error as NodeJS.ErrnoException).code === "ESRCH") return undefined;
    throw error; // Unknown identity is not permission to signal or to report exit.
  }
};
const linuxProcesses = (): LinuxGroupMember[] =>
  fs.readdirSync("/proc").flatMap((entry) => {
    if (!/^\d+$/.test(entry)) return [];
    const member = linuxGroupMember(Number(entry));
    return member && member.state !== "Z" && member.state !== "X" ? [member] : [];
  });
const STOP_TERM_MS = 2_000;
const STOP_KILL_MS = 2_000;
const stopDelay = () => new Promise<void>((resolve) => setTimeout(resolve, 20));

export const spawnDetached = async (
  workerPath: string,
  workerArguments: string[],
  cwd: string,
  authority?: Pick<AgentTransportLaunch, "signal" | "authorize">,
  /** Ordinary workers need time to run their five-second execution-child cleanup. */
  termGraceMs = STOP_TERM_MS,
  executionCustodian = false,
): Promise<{ pid: number; stop(): Promise<void>; isAlive(): Promise<boolean> }> => {
  const runtime = await resolveScriptRuntime(runtimeOptionsForWorker(workerPath));
  assertTransportLaunchAllowed(authority);
  // The new tree-custody protocol is unsupported on Windows. Even an internal
  // caller requesting it must get only the legacy native worker-exit contract.
  const tracksExecution = executionCustodian && process.platform !== "win32";
  const child = spawn(runtime, [workerPath, ...workerArguments], {
    cwd,
    detached: process.platform !== "win32",
    stdio: tracksExecution ? ["ignore", "ignore", "ignore", "ipc"] : "ignore",
  });
  if (!child.pid) throw new Error("Failed to launch Fabric worker process");
  const pid = child.pid;
  // Exit is latched: after the worker/group empties its numeric id is not identity.
  let exited = false;
  child.once("exit", () => { exited = true; });
  child.unref();
  child.channel?.unref();
  let executionPending = false;
  child.on("message", (message: unknown) => {
    if (!message || typeof message !== "object" || !("type" in message)) return;
    if (message.type === "fabric-execution-custody") {
      // The real worker cannot spawn execution until we acknowledge custody.
      executionPending = true;
      if (child.connected) child.send({ type: "fabric-execution-custody-ack" }, () => undefined);
    } else if (message.type === "fabric-execution-started" && "pid" in message &&
      Number.isSafeInteger(message.pid) && Number(message.pid) > 0 && "started" in message && typeof message.started === "string") {
      // This private native channel belongs to our worker. Retain its reported
      // birth even if the custodian already died and the child was reparented.
      // members() still revalidates before signaling any reported group.
      if (process.platform === "linux") {
        owned.set(Number(message.pid), message.started);
        groups.add(Number(message.pid));
      }
    } else if (message.type === "fabric-execution-settled") executionPending = false;
  });
  let birth: LinuxGroupMember | undefined;
  try { birth = process.platform === "linux" ? linuxGroupMember(pid) : undefined; } catch { /* stop fails closed on unknown identity */ }
  let stopping: Promise<void> | undefined;
  const owned = new Map<number, string>();
  if (birth) owned.set(pid, birth.started);
  const groups = new Set([pid]);
  let portableUncertain = false;
  const portableMembers = async (): Promise<Array<{ pid: number; group: number }>> => {
    const { stdout } = await executeFile("ps", ["-axo", "pid=,ppid=,pgid=,stat="], { timeoutMs: 1_000 });
    const snapshot = stdout.split("\n").flatMap(line => {
      if (!line.trim()) return [];
      const fields = line.trim().split(/\s+/);
      const [member, parent, group, state] = fields;
      if (fields.length !== 4 || !/^\d+$/.test(member!) || !/^\d+$/.test(parent!) || !/^\d+$/.test(group!)) {
        throw new Error(`Cannot confirm ownership/exit from process snapshot for ${pid}`);
      }
      return state && !state.startsWith("Z") && !state.startsWith("X") && (!exited || Number(member) !== pid)
        ? [{ pid: Number(member), parent: Number(parent), group: Number(group) }] : [];
    });
    if (!exited) {
      const descendants = new Set([pid]);
      let changed = true;
      while (changed) {
        changed = false;
        for (const member of snapshot) if (descendants.has(member.parent) && !descendants.has(member.pid)) {
          descendants.add(member.pid); groups.add(member.group); changed = true;
        }
      }
    }
    // Without birth identities these are cleanup obligations, NEVER permission
    // to signal an extra group or kill its custodian before its own drain ends.
    return snapshot.filter(member => groups.has(member.group));
  };
  let stopped = false;
  const members = (): LinuxGroupMember[] => {
    const snapshot = linuxProcesses().filter((member) => !exited || member.pid !== pid);
    // Retain detached execution groups BEFORE their custodian can die/reparent
    // them. An edge is admitted only while its parent's birth still matches.
    let changed = true;
    while (changed) {
      changed = false;
      for (const member of snapshot) {
        if (owned.get(member.pid) === member.started) continue;
        const parent = snapshot.find((candidate) => candidate.pid === member.parent);
        if (!parent || owned.get(parent.pid) !== parent.started) continue;
        const currentParent = linuxGroupMember(parent.pid);
        const current = linuxGroupMember(member.pid);
        if (currentParent?.started !== parent.started || current?.started !== member.started || current.parent !== parent.pid) {
          throw new Error(`Cannot confirm ownership/exit of Fabric descendant ${member.pid}`);
        }
        owned.set(member.pid, member.started);
        changed = true;
      }
    }
    for (const member of snapshot) {
      if (owned.get(member.pid) !== member.started || groups.has(member.group)) continue;
      // Never signal a foreign group an owned process joined. Detached workers
      // create their own group; its leader must itself be an owned birth.
      const leader = snapshot.find((candidate) => candidate.pid === member.group);
      if (!leader || owned.get(leader.pid) !== leader.started) {
        throw new Error(`Cannot confirm ownership/exit of Fabric process group ${member.group}`);
      }
      groups.add(member.group);
    }
    const current = snapshot.filter((member) => groups.has(member.group));
    for (const group of groups) {
      const groupMembers = current.filter((member) => member.group === group);
      if (groupMembers.length && !groupMembers.some((member) => owned.get(member.pid) === member.started)) {
        throw new Error(`Cannot confirm ownership/exit of Fabric process group ${group}`);
      }
      for (const member of groupMembers) owned.set(member.pid, member.started);
    }
    return current;
  };
  const stop = async (): Promise<void> => {
    if (stopped) return;
    // Capture group identities BEFORE TERM, while the owned leader still pins it.
    // A leader can exit before its refusing child. Retain that child's birth, not
    // just a group number, and never adopt a recycled group with no owned member.
    if (process.platform === "linux") members();
    else if (process.platform !== "win32") await portableMembers();
    const settled = async (): Promise<boolean> => {
      if (process.platform === "linux") return members().length === 0 && exited && !(executionPending && groups.size === 1);
      const remaining = process.platform === "win32" ? [] : await portableMembers();
      if (!exited || executionPending || portableUncertain) return false;
      return remaining.length === 0;
    };
    const signal = (value: NodeJS.Signals): void => {
      const targets = process.platform === "linux"
        ? [...new Set(members().filter(member => value !== "SIGTERM" || exited || member.group === pid).map(member => -member.group))]
        : exited ? [] : [process.platform === "win32" ? pid : -pid];
      // TERM the live custodian, not its execution first: it must record stop
      // intent before the child can close. Escalation owns all retained groups.
      // Escalate execution groups before their root custodian. Custody remains
      // retained even if a child refuses KILL or another group appears meanwhile.
      targets.sort((left, right) => left === -pid ? 1 : right === -pid ? -1 : 0);
      for (const target of targets) {
        // Refresh birth anchors immediately before EACH signal, not only once
        // before signalling several independently detached execution groups.
        if (process.platform === "linux" && !members().some((member) => member.group === -target)) continue;
        try { process.kill(target, value); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; }
      }
    };
    const wait = async (ms: number): Promise<boolean> => {
      const deadline = Date.now() + ms;
      do { if (await settled()) return true; await stopDelay(); } while (Date.now() < deadline);
      return settled();
    };
    // POSIX custodians drain cooperatively on TERM. Windows uses only its
    // legacy native worker stop; no tree receipt or custody IPC is supported.
    signal("SIGTERM");
    if (await wait(termGraceMs)) return;
    if (process.platform !== "linux" && (executionPending || portableUncertain || groups.size > 1)) {
      throw new Error(`Fabric worker ${pid} execution exit unconfirmed; retaining custodian without birth-safe escalation`);
    }
    signal("SIGKILL");
    if (!(await wait(STOP_KILL_MS))) throw new Error(`Fabric worker ${pid} did not exit after bounded SIGTERM/SIGKILL cleanup`);
  };
  return {
    pid,
    async stop() {
      if (stopping) return stopping;
      const pending = stop();
      stopping = pending;
      try { await pending; stopped = true; } finally { stopping = undefined; }
    },
    async isAlive() {
      // A dead custodian is not proof its retained execution groups stopped.
      // The manager must not use it to admit an overlapping replacement.
      if (exited) return process.platform === "linux" ? members().length > 0 || (executionPending && groups.size === 1)
        : executionPending || portableUncertain || (process.platform !== "win32" && (await portableMembers()).length > 0);
      if (processIsAlive(pid)) {
        // Retain observed descendants before a launcher can exit ahead of its Pi child.
        if (process.platform === "linux") { try { members(); } catch { /* stop must fail closed */ } }
        else if (process.platform !== "win32") { try { await portableMembers(); } catch { /* unknown descendants cannot authorize escalation */ portableUncertain = true; } }
        return true;
      }
      // Gone once is gone for good: the probe can see the exit before the "exit" event
      // (Windows), and any later answer for this number is another process.
      exited = true;
      return process.platform === "linux" ? members().length > 0 || (executionPending && groups.size === 1)
        : executionPending || portableUncertain || (process.platform !== "win32" && (await portableMembers()).length > 0);
    },
  };
};
