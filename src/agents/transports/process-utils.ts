import { execFile, spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { AgentTransportLaunch } from "../types.js";
import { assertTransportLaunchAllowed } from "./launch-authority.js";
import { terminateWindowsTree } from "../../child-process-tree.js";
import { spawnDetached as spawnDetachedLegacy } from "./process-utils-legacy.js";

/** A bounded tree observation ended without proof of descendant exit. */
export class ProcessTreeCustodyUnconfirmedError extends Error {
  readonly code = "PROCESS_TREE_CUSTODY_UNCONFIRMED" as const;
  constructor(readonly descendants: number) {
    super(`custody unconfirmed: ${descendants} descendants may remain`);
    this.name = "ProcessTreeCustodyUnconfirmedError";
  }
}

export interface ExecFileResult {
  stdout: string;
  stderr: string;
}

export const executeFile = (
  command: string,
  args: string[],
  options: { cwd?: string; timeoutMs?: number; signal?: AbortSignal; env?: NodeJS.ProcessEnv; killSignal?: NodeJS.Signals } = {},
): Promise<ExecFileResult> =>
  new Promise((resolve, reject) => {
    if (options.signal?.aborted) {
      reject(new Error("Command cancelled before execution"));
      return;
    }
    let closed = false;
    let cancelled = false;
    let outcome: { error: Error | null; stdout: string; stderr: string } | undefined;
    const finish = (): void => {
      if (!closed || !outcome) return;
      if (outcome.error) {
        Object.assign(outcome.error, { stdout: outcome.stdout, stderr: outcome.stderr });
        reject(outcome.error);
      } else resolve({ stdout: outcome.stdout, stderr: outcome.stderr });
    };
    const child = execFile(
      command,
      args,
      {
        encoding: "utf8",
        maxBuffer: 10 * 1024 * 1024,
        ...(options.cwd ? { cwd: options.cwd } : {}),
        ...(options.env ? { env: options.env } : {}),
        ...(options.timeoutMs ? { timeout: options.timeoutMs } : {}),
        // Query callers opt into SIGKILL; preserve cooperative termination for
        // existing mutating commands (for example git releasing its lock files).
        killSignal: options.killSignal ?? "SIGTERM",
      },
      (error, stdout, stderr) => {
        outcome = { error: cancelled ? new Error("Command cancelled during execution") : error, stdout, stderr };
        finish();
      },
    );
    // execFile does not forward killSignal to spawn's AbortSignal handler in
    // supported Node versions. Own cancellation so the selected signal is used,
    // and settle only after native close (also on Windows).
    const abort = (): void => {
      cancelled = true;
      // Mirror execFile's timeout teardown: inherited pipe handles must not
      // keep native close pending after the query process itself was killed.
      child.stdout?.destroy(); child.stderr?.destroy();
      child.kill(options.killSignal ?? "SIGTERM");
    };
    options.signal?.addEventListener("abort", abort, { once: true });
    child.once("close", () => {
      options.signal?.removeEventListener("abort", abort);
      closed = true; finish();
    });
    if (options.signal?.aborted) abort();
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
const TREE_CENSUS_MIN_SPACING_MS = 60_000;
const stopDelay = () => new Promise<void>((resolve) => setTimeout(resolve, 20));

export const spawnDetached = async (
  workerPath: string,
  workerArguments: string[],
  cwd: string,
  authority?: Pick<AgentTransportLaunch, "signal" | "authorize" | "onUnconfirmedExit">,
  environment?: NodeJS.ProcessEnv,
  scope?: { executable: string; slice: string; warn: (reason: string) => void },
  /** Ordinary workers need time to run their five-second execution-child cleanup. */
  termGraceMs = STOP_TERM_MS,
  executionCustodian = false,
): Promise<{ pid: number; closed: Promise<void>; treeClosed?: Promise<void>; stop(): Promise<void>; isAlive(): Promise<boolean>; lostContact(): string | undefined; stopDebt?(): string | undefined; waitForClose(): Promise<void> }> => {
  // No PR observer or timer is created on the unscoped/unsupported path.
  const scopedLinux = Boolean(scope && process.platform === "linux");
  if (!scopedLinux) {
    return spawnDetachedLegacy(workerPath, workerArguments, cwd, authority, environment, undefined, termGraceMs, executionCustodian);
  }
  const runtime = await resolveScriptRuntime(runtimeOptionsForWorker(workerPath));
  const treeOwner = process.platform === "linux" ? await import("../../residency/launcher-owner.js") : undefined;
  assertTransportLaunchAllowed(authority);
  // The new tree-custody protocol is unsupported on Windows. Even an internal
  // caller requesting it must get only the legacy native worker-exit contract.
  const tracksExecution = executionCustodian && process.platform !== "win32";
  // Scope admission execs in place: the captured PID and custody IPC stay owned.
  const scopeRoot = scope ? fs.mkdtempSync(path.join(os.tmpdir(), "fabric-scope-")) : undefined;
  const marker = scopeRoot ? path.join(scopeRoot, "admitted") : undefined;
  const scopeUnit = scopeRoot ? `${path.basename(scopeRoot)}.scope` : undefined;
  const markerIdentity = `admitted ${scopeUnit}`;
  const markerIsAdmitted = (): boolean => {
    if (!marker) return false;
    let fd: number | undefined;
    try {
      // A filename notification is only a wakeup. Authenticate the actual
      // marker, including its launch identity, on every admission check.
      const stat = fs.lstatSync(marker);
      if (!stat.isFile() || stat.uid !== process.getuid?.()) return false;
      fd = fs.openSync(marker, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
      const opened = fs.fstatSync(fd);
      if (!opened.isFile() || opened.uid !== stat.uid || opened.dev !== stat.dev || opened.ino !== stat.ino
        || opened.size !== Buffer.byteLength(markerIdentity)) return false;
      return fs.readFileSync(fd, "utf8") === markerIdentity;
    } catch { return false; }
    finally { if (fd !== undefined) fs.closeSync(fd); }
  };
  const child = spawn(scope?.executable ?? runtime, scope ? [
    "--user", "--scope", `--slice=${scope.slice}`, "--quiet", "--collect", `--unit=${scopeUnit}`, "--",
    "/bin/sh", "-c", 'while IFS= read -r line; do printf "%s\\n" "$line"; done < /proc/self/cgroup > "$1.cgroup" || exit 125; printf "admitted %s" "$2" > "$1" || exit 125; shift 2; exec "$@"',
    "fabric-scope", marker!, scopeUnit!, runtime, workerPath, ...workerArguments,
  ] : [workerPath, ...workerArguments], {
    cwd,
    ...(environment ? { env: environment } : {}),
    detached: process.platform !== "win32",
    stdio: tracksExecution ? ["ignore", "ignore", "ignore", "ipc"] : "ignore",
  });
  let spawnError: Error | undefined;
  child.once("error", error => { spawnError = error; });
  if (!child.pid) {
    await new Promise<void>(resolve => child.once("close", () => resolve()));
    if (!scope) throw spawnError ?? new Error("Failed to launch Fabric worker process");
    fs.rmSync(scopeRoot!, { recursive: true, force: true });
    assertTransportLaunchAllowed(authority);
    scope.warn(spawnError?.message ?? "systemd-run did not launch");
    return spawnDetached(workerPath, workerArguments, cwd, authority, environment, undefined, termGraceMs, executionCustodian);
  }
  const pid = child.pid;
  // Exit is latched: after the worker/group empties its numeric id is not identity.
  let exited = false;
  let force: ReturnType<typeof setTimeout> | undefined;
  child.once("exit", () => { exited = true; clearTimeout(force); });
  const closed = new Promise<void>((resolve) => child.once("close", () => { exited = true; clearTimeout(force); resolve(); }));
  let stopping: Promise<void> | undefined;
  let lost: string | undefined;
  const unconfirmed = (reason: string): void => {
    if (lost !== undefined) return;
    lost = reason;
    try { authority?.onUnconfirmedExit?.(reason); } catch { /* transport debt still vetoes release */ }
  };
  child.unref();
  // Bun exposes an IPC channel without Node's unref method. The child's
  // native unref above is still valid; optional channel APIs are not custody.
  child.channel?.unref?.();
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
  let birthUnknown = false;
  try { birth = process.platform === "linux" ? linuxGroupMember(pid) : undefined; } catch { birthUnknown = true; }
  // Share the original birth anchor with sampled cleanup. A second observation
  // must not adopt a replacement process when the captured worker was absent.
  const ownedTree = treeOwner ? { processes: new Map<number, import("../../residency/launcher-owner.js").OwnedProcess>(
    birth ? [[pid, { pid, processStartTime: birth.started, ppid: birth.parent, state: birth.state }]] : [],
  ) } : undefined;
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
  let stopFailed = false;
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
      let expired = false;
      const timer = setTimeout(() => { expired = true; }, ms);
      try {
        do { if (await settled()) return true; await stopDelay(); } while (!expired);
        return settled();
      } finally { clearTimeout(timer); }
    };
    // POSIX custodians drain cooperatively on TERM. Windows uses only its
    // legacy native worker stop; no tree receipt or custody IPC is supported.
    signal("SIGTERM");
    if (await wait(termGraceMs)) return;
    if (process.platform !== "linux" && (executionPending || portableUncertain || groups.size > 1)) {
      throw new Error(`Fabric worker ${pid} execution exit unconfirmed; retaining custodian without birth-safe escalation`);
    }
    // A forcibly killed custodian cannot attest that all separately grouped
    // native execution was drained. Birth-checked escalation still joins every
    // observed group, but must not erase this immutable receipt debt.
    if (tracksExecution && !exited) unconfirmed(`POSIX worker tree termination is unconfirmed after ${termGraceMs}ms grace`);
    signal("SIGKILL");
    if (!(await wait(STOP_KILL_MS))) {
      if (tracksExecution) {
        unconfirmed(`Fabric worker ${pid} did not confirm execution exit after bounded SIGTERM/SIGKILL cleanup`);
        return;
      }
      throw new Error(`Fabric worker ${pid} did not exit after bounded SIGTERM/SIGKILL cleanup`);
    }
  };
  // Only the scoped branch publishes a passive tree-empty receipt. After
  // populated-0, retained execution groups get one final deadline census.
  // Unscoped workers returned through the unchanged legacy path above.
  const posix = process.platform !== "win32";
  let treeConfirmed = false;
  let treeFinished = false;
  let nativeClosed = false;
  // Receipt existence, not merely requested scope admission, enables events.
  let treeObservationReady = false;
  let censusTimer: ReturnType<typeof setTimeout> | undefined;
  let scopeWatcher: fs.FSWatcher | undefined;
  let scopeEvents: string | undefined;
  let scopeEventsFd: number | undefined;
  let observingTree = false;
  let censusStarted = false;
  let censusDeadlineReached = false;
  const scopeIsEmpty = (): boolean => {
    if (scopeEventsFd === undefined) throw new Error("Owned scope events were not opened");
    const buffer = Buffer.alloc(4096);
    // Positioned reads keep a retained descriptor useful after scope removal,
    // and never consume the next populated notification's file offset.
    const size = fs.readSync(scopeEventsFd, buffer, 0, buffer.length, 0);
    const populated = buffer.toString("utf8", 0, size).match(/^populated ([01])$/m)?.[1];
    if (populated === undefined) throw new Error("Owned scope populated receipt is unreadable");
    return populated === "0";
  };
  let resolveTree!: () => void;
  let rejectTree!: (error: unknown) => void;
  let treeClosed: Promise<void> | undefined;
  const finishTree = (error?: unknown): void => {
    if (treeFinished) return;
    treeFinished = true;
    clearTimeout(censusTimer);
    scopeWatcher?.close();
    if (scopeEventsFd !== undefined) fs.closeSync(scopeEventsFd);
    scopeEventsFd = undefined;
    scopeEvents = undefined;
    if (error !== undefined) rejectTree(error);
    else { treeConfirmed = true; resolveTree(); }
  };
  const observeTree = async (atDeadline = false): Promise<void> => {
    if (!posix || treeFinished || observingTree || !nativeClosed || !treeObservationReady) return;
    // Scope changes may deliver populated-0, but cannot restart a pending
    // deadline census.
    if (!atDeadline && (censusTimer || (!scopeEvents && censusStarted))) return;
    observingTree = true;
    // cgroup.events is passive even after primary close. Never replace a live
    // owned scope's populated receipt with a primary-PID absence observation.
    try {
      if (scopeEvents && !scopeIsEmpty()) return;
      censusStarted = true;
      const remaining = process.platform === "linux" ? members().length : (await portableMembers()).length;
      const uncertain = process.platform === "linux"
        ? executionPending && groups.size === 1 : executionPending || portableUncertain;
      if (remaining === 0 && !uncertain) { finishTree(); return; }
      if (atDeadline || censusDeadlineReached) {
        // A pending execution receipt represents at least one possible child,
        // even when the census cannot name it. Never turn uncertainty into exit.
        const error = new ProcessTreeCustodyUnconfirmedError(Math.max(remaining, uncertain ? 1 : 0));
        unconfirmed(error.message);
        finishTree(error);
        return;
      }
      // A populated-zero scope can retain separately grouped execution too.
      // Give those obligations the same ONE final observation, never a loop.
      censusTimer = setTimeout(function finalTreeCensus() {
        censusTimer = undefined;
        censusDeadlineReached = true;
        void observeTree(true);
      }, Math.max(TREE_CENSUS_MIN_SPACING_MS, termGraceMs + STOP_KILL_MS));
      censusTimer.unref?.();
    } catch (error) { unconfirmed(`Owned scope observation failed: ${String(error)}`); finishTree(error); }
    finally { observingTree = false; }
  };
  const armTreeObservation = async (): Promise<boolean> => {
    if (treeObservationReady) return true;
    if (!posix || !scopeUnit || !marker || !scope) return false;
    // A unified entry in a hybrid hierarchy is not a v2-only receipt. The
    // admitted shim alone is also insufficient: verify the captured live PID.
    const unifiedPath = (text: string): string => {
      const lines = text.trim().split("\n");
      if (lines.length !== 1 || !lines[0]!.startsWith("0::/")) throw new Error("No v2-only cgroup receipt");
      return lines[0]!.slice(3);
    };
    let armError: unknown;
    try {
      const cgroup = unifiedPath(fs.readFileSync(`/proc/${pid}/cgroup`, "utf8"));
      if (unifiedPath(fs.readFileSync(`${marker}.cgroup`, "utf8")) !== cgroup
        || path.posix.normalize(cgroup) !== cgroup
        || path.posix.basename(cgroup) !== scopeUnit
        || path.posix.basename(path.posix.dirname(cgroup)) !== scope.slice) {
        throw new Error("Cgroup receipt does not match the launched scope/slice");
      }
      if (!birth || linuxGroupMember(pid)?.started !== birth.started) throw new Error("Scope worker birth is unverified");
      const directory = path.resolve("/sys/fs/cgroup", `.${cgroup}`);
      if (!directory.startsWith("/sys/fs/cgroup/")) throw new Error("Owned scope cgroup path escaped its mount");
      const ownedDirectory = (): boolean => {
        const stat = fs.lstatSync(directory);
        return stat.isDirectory() && stat.uid === process.getuid?.();
      };
      if (!ownedDirectory() || fs.statfsSync(directory).type !== 0x63677270) throw new Error("Scope is not our UID-owned cgroup-v2 directory");
      scopeEvents = path.join(directory, "cgroup.events");
      scopeEventsFd = fs.openSync(scopeEvents, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
      const changed = (): void => {
        if (!treeObservationReady || treeFinished) return;
        try { scopeIsEmpty(); void observeTree(); }
        catch (error) { unconfirmed(`Owned scope observation failed: ${String(error)}`); finishTree(error); }
      };
      // Open first, then subscribe before the initial read to fence last exit.
      // A constructor failure or queued arm error yields no public receipt.
      scopeWatcher = fs.watch(scopeEvents, { persistent: false }, changed);
      scopeWatcher.on("error", error => {
        if (!treeObservationReady) armError = error;
        else { unconfirmed(`Owned scope watcher failed: ${String(error)}`); finishTree(error); }
      });
      scopeIsEmpty();
      await new Promise<void>(resolve => setImmediate(resolve));
      if (armError !== undefined) throw armError;
      if (!ownedDirectory() || unifiedPath(fs.readFileSync(`/proc/${pid}/cgroup`, "utf8")) !== cgroup
        || linuxGroupMember(pid)?.started !== birth.started) throw new Error("Scope identity changed during watcher admission");
      scopeIsEmpty(); // Revalidate after the asynchronous arm boundary, before publishing any receipt.
      treeClosed = new Promise<void>((resolve, reject) => { resolveTree = resolve; rejectTree = reject; });
      // Preserve failures for the subscriber without an admission-gap rejection.
      void treeClosed.catch(() => undefined);
      treeObservationReady = true;
      void observeTree();
      return true;
    } catch {
      // Scope launch is not replayed. This SAME worker keeps legacy checked
      // monitoring, with no treeClosed receipt and no event census deadline.
      scopeWatcher?.close();
      scopeWatcher = undefined;
      if (scopeEventsFd !== undefined) fs.closeSync(scopeEventsFd);
      scopeEventsFd = undefined;
      scopeEvents = undefined;
      return false;
    }
  };
  void closed.then(() => { nativeClosed = true; void observeTree(); });

  const handle = {
    pid,
    closed,
    lostContact: () => lost,
    stopDebt: () => stopFailed ? undefined : lost,
    async waitForClose() {
      let deadline: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([closed, new Promise<void>(resolve => {
          deadline = setTimeout(() => {
            unconfirmed("Owned process worker did not confirm native close within 7000ms");
            resolve();
          }, 7_000);
        })]);
      } finally { clearTimeout(deadline); }
    },
    stop() {
      if (stopping) return stopping;
      stopFailed = false;
      const pending = (async () => {
        // Retain the PR's observed detached descendants before TERM can reparent them.
        if (ownedTree && treeOwner) treeOwner.captureDescendants(ownedTree);
        if (process.platform !== "win32") {
          if (process.platform === "linux" && !birth) {
            // A captured native handle can lag /proc absence. Bound its close
            // without adopting a new birth or an unowned surviving group. In
            // particular, an unreadable birth is NOT this absent-worker case.
            const signalAbsent = (value: NodeJS.Signals): void => {
              if (birthUnknown || linuxGroupMember(pid) || members().length) {
                throw new Error(`Cannot confirm ownership/exit of Fabric process group ${pid}`);
              }
              try { process.kill(-pid, value); }
              catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; }
            };
            if (!exited) {
              unconfirmed("Owned process worker birth/tree exit is unconfirmed");
              signalAbsent("SIGTERM");
              force = setTimeout(() => {
                if (exited) return;
                try { signalAbsent("SIGKILL"); } catch { stopFailed = true; /* never signal unknown identity */ }
              }, 6_000);
            }
          } else {
            // Preserve birth-checked POSIX execution-group drain and retryable
            // identity failures. Never turn a failed ownership check into exit.
            await stop();
          }
        }
        let deadline: ReturnType<typeof setTimeout> | undefined;
        try {
          await Promise.race([
            (async () => {
              if (process.platform === "win32" && !exited) {
                // Helper failure records immutable debt before parent-only fallback.
                // Attempt close and captured native close are separate obligations.
                await new Promise<void>(resolve => {
                  void terminateWindowsTree(child, unconfirmed, resolve);
                });
              }
              // Exit/probe absence alone is not captured native close.
              await closed;
            })(),
            new Promise<void>(resolve => {
              deadline = setTimeout(() => {
                unconfirmed("Owned process worker did not confirm tree/native close within 7000ms");
                resolve();
              }, 7_000);
            }),
          ]);
          // Sampling cleanup supplements, but never replaces, the confirmed-exit
          // custody and birth-checked group drain above. Native-close debt stays latched.
          if (ownedTree && treeOwner) await treeOwner.stopObservedDescendants(ownedTree, pid);
          stopped = true;
        } finally { clearTimeout(force); clearTimeout(deadline); }
      })();
      stopping = pending;
      // POSIX ownership/exit failures remain retryable on the exact same handle.
      void pending.catch(() => { stopFailed = true; if (stopping === pending) stopping = undefined; });
      return pending;
    },
    async isAlive() {
      if (treeConfirmed) return false;
      // The close/deadline census owns this observation. A manager close wake
      // must not duplicate it, nor turn an early descendant exit into a probe
      // loop. Pending custody is conservatively alive until its one receipt.
      // Explicit stop still owns the existing birth-checked cleanup drain.
      if (nativeClosed && censusStarted && !treeFinished && !stopping) return true;
      if (exited && scopeEvents && !scopeIsEmpty()) return true;
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
  if (scope && marker) {
    // Only a verified arm may publish event custody. Validation failure keeps
    // this same worker on checked legacy monitoring, without replay or receipt.
    const admittedHandle = async () => await armTreeObservation() ? { ...handle, treeClosed: treeClosed! } : handle;
    try {
      const admitted = await (async (): Promise<boolean> => {
        let watcher: fs.FSWatcher | undefined;
        let deadline: ReturnType<typeof setTimeout> | undefined;
        let finish!: (admitted: boolean) => void;
        const ended = (): void => finish(false);
        try {
          return await new Promise<boolean>(resolve => {
            let settled = false;
            finish = admitted => {
              if (settled) return;
              settled = true;
              resolve(admitted);
            };
            // Subscribe to the DIRECTORY before checking once: the shim may
            // create the marker either side of the watcher's admission race.
            watcher = fs.watch(path.dirname(marker), { persistent: false }, (_event, filename) => {
              if (filename?.toString() === path.basename(marker) && markerIsAdmitted()) finish(true);
            });
            watcher.once("error", ended);
            child.once("close", ended);
            authority?.signal?.addEventListener("abort", ended, { once: true });
            if (markerIsAdmitted()) finish(true);
            else if (nativeClosed || authority?.signal?.aborted) finish(false);
            else if (!settled) deadline = setTimeout(ended, 5_000);
          });
        } catch {
          // An unavailable directory watch uses the checked legacy fallback
          // below, never a marker polling loop or an unverified tree receipt.
          return false;
        } finally {
          clearTimeout(deadline);
          watcher?.close();
          child.removeListener("close", ended);
          authority?.signal?.removeEventListener("abort", ended);
        }
      })();
      if (admitted) return await admittedHandle();
      if (!nativeClosed) await handle.stop();
      if (handle.lostContact()) throw new Error(handle.lostContact());
      if (await handle.isAlive()) throw new Error("Scope termination is unconfirmed; custody retained");
      assertTransportLaunchAllowed(authority);
      if (markerIsAdmitted()) return await admittedHandle(); // admitted during teardown: never replay
      scope.warn(spawnError?.message ?? "systemd-run failed or scope admission timed out");
      return await spawnDetached(workerPath, workerArguments, cwd, authority, environment, undefined, termGraceMs, executionCustodian);
    } finally { fs.rmSync(scopeRoot!, { recursive: true, force: true }); }
  }
  return handle;
};
