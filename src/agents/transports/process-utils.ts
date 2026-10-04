import { execFile, spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { AgentTransportLaunch } from "../types.js";
import { assertTransportLaunchAllowed } from "./launch-authority.js";
import { terminateWindowsTree } from "../../child-process-tree.js";

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

export const spawnDetached = async (
  workerPath: string,
  workerArguments: string[],
  cwd: string,
  authority?: Pick<AgentTransportLaunch, "signal" | "authorize" | "onUnconfirmedExit">,
  environment?: NodeJS.ProcessEnv,
  scope?: { executable: string; slice: string; warn: (reason: string) => void },
): Promise<{ pid: number; closed: Promise<void>; stop(): Promise<void>; isAlive(): Promise<boolean>; lostContact(): string | undefined; waitForClose(): Promise<void> }> => {
  const runtime = await resolveScriptRuntime(runtimeOptionsForWorker(workerPath));
  const treeOwner = process.platform === "linux" ? await import("../../residency/launcher-owner.js") : undefined;
  assertTransportLaunchAllowed(authority);
  // --scope execs its command in place. The shell records successful scope admission
  // then execs the runtime in place too: captured child.pid remains worker PID/PGID.
  // A marker distinguishes systemd failure from a worker that legitimately exits 1.
  const scopeRoot = scope ? fs.mkdtempSync(path.join(os.tmpdir(), "fabric-scope-")) : undefined;
  const marker = scopeRoot ? path.join(scopeRoot, "admitted") : undefined;
  const child = spawn(scope?.executable ?? runtime, scope ? [
    "--user", "--scope", `--slice=${scope.slice}`, "--quiet", "--collect", "--",
    "/bin/sh", "-c", 'printf admitted > "$1" || exit 125; shift; exec "$@"',
    "fabric-scope", marker!, runtime, workerPath, ...workerArguments,
  ] : [workerPath, ...workerArguments], {
    cwd,
    ...(environment ? { env: environment } : {}),
    detached: process.platform !== "win32",
    stdio: "ignore",
  });
  let spawnError: Error | undefined;
  child.once("error", error => { spawnError = error; });
  if (!child.pid) {
    if (!scope) throw new Error("Failed to launch Fabric worker process");
    await new Promise<void>(resolve => child.once("close", () => resolve()));
    fs.rmSync(scopeRoot!, { recursive: true, force: true });
    scope.warn(spawnError?.message ?? "systemd-run did not launch");
    return spawnDetached(workerPath, workerArguments, cwd, authority, environment);
  }
  const pid = child.pid;
  // Once the worker exited, its numeric id is no identity: after its group empties, the id
  // can name an unrelated process (group). So nothing is signalled or probed by number then.
  // ponytail: descendants an exited worker left in its group are not signalled; liveness
  // (and so relaunch) is about the worker itself.
  let exited = false;
  let force: ReturnType<typeof setTimeout> | undefined;
  child.once("exit", () => { exited = true; clearTimeout(force); });
  const closed = new Promise<void>((resolve) => child.once("close", () => resolve()));
  let stopping: Promise<void> | undefined;
  let lost: string | undefined;
  const unconfirmed = (reason: string): void => {
    if (lost !== undefined) return;
    lost = reason;
    try { authority?.onUnconfirmedExit?.(reason); } catch { /* transport debt still vetoes release */ }
  };
  const ownedTree = treeOwner?.observeProcessTree(pid);
  const stopNative = async (): Promise<void> => {
    let deadline: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        (async () => {
          if (!exited) {
            // A Windows parent-only kill cannot join its native descendants.
            // Publish helper uncertainty before its fallback can emit exit.
            if (process.platform === "win32") await new Promise<void>(resolve => {
              // Join the actual helper attempt, even when it failed. Failure
              // publishes immutable lost-contact debt BEFORE this join ends;
              // the owner retains files and native admission indefinitely.
              // Waiting out the whole seven-second bound after helper close
              // only delays logical stop, without adding any exit evidence.
              void terminateWindowsTree(child, unconfirmed, resolve);
            });
            else {
              try { process.kill(-pid, "SIGTERM"); }
              catch { /* still require captured native close */ }
              // The worker gives its separately grouped native child 5000ms.
              // Leave another second for TERM delivery, child KILL/close,
              // and worker exit before escalating the still-live worker.
              force = setTimeout(() => {
                if (exited) return; // Never signal an exited/reused numeric identity.
                // Native Pi can own a separate group. Forced worker exit
                // cannot prove that its graceful descendant teardown ran.
                unconfirmed("POSIX worker tree termination is unconfirmed after 6000ms grace");
                try { process.kill(-pid, "SIGKILL"); }
                catch { /* deadline records an unconfirmed exit */ }
              }, 6_000);
            }
          }
          // Exit/probe absence alone is not native close. However, a stuck
          // worker (or unknown Windows tree) must not hang timeout/shutdown.
          await closed;
        })(),
        new Promise<void>(resolve => {
          deadline = setTimeout(() => {
            unconfirmed("Owned process worker did not confirm tree/native close within 7000ms");
            resolve();
          }, 7_000);
        }),
      ]);
    } finally {
      clearTimeout(force);
      clearTimeout(deadline);
    }
  };
  child.unref();
  const handle = {
    pid,
    closed,
    lostContact: () => lost,
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
      return stopping ??= (async () => {
        // Snapshot before native TERM can reparent detached shells, but let the
        // worker handle stop and join its child first. Signalling Pi concurrently
        // can publish a child-exit failure before the worker handles our TERM.
        if (ownedTree && treeOwner) treeOwner.captureDescendants(ownedTree);
        const results = await Promise.allSettled([stopNative()]);
        // Native close alone is not tree exit. Join every captured survivor even
        // if the bounded native stop failed; never let teardown reclassify stop.
        if (ownedTree && treeOwner) results.push(...await Promise.allSettled([
          treeOwner.stopObservedDescendants(ownedTree, pid),
        ]));
        for (const result of results) if (result.status === "rejected") throw result.reason;
        if (process.platform === "linux" && lost !== undefined) throw new Error(lost);
      })();
    },
    async isAlive() {
      if (exited) return false;
      if (processIsAlive(pid)) return true;
      // Gone once is gone for good: the probe can see the exit before the "exit" event
      // (Windows), and any later answer for this number is another process.
      exited = true;
      return false;
    },
  };
  if (scope && marker) {
    let nativeClosed = false;
    void closed.then(() => { nativeClosed = true; });
    const admissionDeadline = Date.now() + 5_000;
    try {
      while (!fs.existsSync(marker) && !nativeClosed && Date.now() < admissionDeadline && !authority?.signal?.aborted) {
        await new Promise<void>(resolve => setTimeout(resolve, 10));
      }
      if (fs.existsSync(marker)) return handle;
      // Join the captured close before any fallback. Unconfirmed custody vetoes
      // a replacement just as it does for an ordinary owned worker.
      if (!nativeClosed) await handle.stop();
      if (handle.lostContact()) throw new Error(handle.lostContact());
      assertTransportLaunchAllowed(authority);
      if (fs.existsSync(marker)) return handle; // admitted during teardown; never replay
      scope.warn(spawnError?.message ?? "systemd-run failed or scope admission timed out");
      return await spawnDetached(workerPath, workerArguments, cwd, authority, environment);
    } finally { fs.rmSync(scopeRoot!, { recursive: true, force: true }); }
  }
  return handle;
};
