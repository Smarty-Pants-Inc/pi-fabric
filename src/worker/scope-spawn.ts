import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import type { ChildProcess, SpawnOptions } from "node:child_process";
import type { Writable } from "node:stream";
import { executeFile, findExecutable } from "../agents/transports/process-utils.js";
import { cgroupCustody, executionCgroups, executionIdentity, processScopePath, scopeDirectory, scopeLauncherEnvironment, type CgroupCustody } from "../process-cgroup.js";

let warned = false;
const warn = (reason: string): void => {
  if (warned) return;
  warned = true;
  console.warn(`[pi-fabric] Linux execution scope: ${reason}; using legacy process-group custody`);
};
export type ScopeLaunch = { executable: string; slice: string; prefix?: "worker" | "execution"; warn: (reason: string) => void; authorize?: () => void; legacy?: boolean; signal?: AbortSignal };
export class ScopeAdmissionError extends Error {
  readonly code = "ERR_SCOPE_ADMISSION";
  constructor(readonly reason: "closed" | "aborted" | "timeout" | "watch" | "launcher", options?: ErrorOptions) {
    super({ closed: "Scope launcher closed before admission", aborted: "Scope launch aborted before admission",
      timeout: "Launcher never confirmed its spawn scope; refusing execution replay", watch: "Scope admission watcher failed",
      launcher: "Scope launcher failed before admission" }[reason], options);
    this.name = "ScopeAdmissionError";
  }
}
const releases = new WeakMap<ChildProcess, () => void>();
/** Release only after native-close/stream listeners and parent custody exist. */
export const releaseScopedChild = (child: ChildProcess): void => { releases.get(child)?.(); releases.delete(child); };

export const spawnScopedExecution = async (
  spawn: (command: string, args: readonly string[], options: SpawnOptions) => ChildProcess,
  command: string, args: readonly string[], options: SpawnOptions,
  parentCanRetainScopes = true,
  configured?: ScopeLaunch,
): Promise<ChildProcess> => {
  const direct = (): ChildProcess => { configured?.authorize?.(); return spawn(command, args, options); };
  if (process.platform !== "linux") return direct();
  if (!parentCanRetainScopes) { warn("parent lacks cgroup custody capability"); return direct(); }
  const executable = configured?.executable ?? findExecutable("systemd-run");
  const warning = configured?.warn ?? warn;
  if (!executable) { warning("systemd-run unavailable"); return direct(); }
  const slice = configured?.slice ?? "app.slice";
  const unit = `fabric-${configured?.prefix ?? "execution"}-${randomUUID()}.scope`;
  // Preserve explicitly configured processSlice placement on cgroup-v1 hosts.
  // Only this existing worker path may trust readiness without a cgroup receipt.
  const legacyScope = configured?.legacy && !fs.existsSync("/sys/fs/cgroup/cgroup.controllers");
  const environment = legacyScope ? options.env : scopeLauncherEnvironment(options.env);
  let directory: string | undefined;
  try {
    if (!legacyScope) {
      // A spawn placement, not a path sampled from a marker (or a migrated PID).
      const systemctl = findExecutable("systemctl");
      if (!systemctl || !fs.existsSync("/sys/fs/cgroup/cgroup.controllers")) throw new Error("cgroup v2 user manager unavailable");
      const { stdout } = await executeFile(systemctl, ["--user", "show", "--property=ControlGroup", "--value", "--", "-.slice"], { ...(environment ? { env: environment } : {}), timeoutMs: 2_000 });
      directory = scopeDirectory(stdout.trim(), slice, unit);
    }
  } catch (error) { warning(String(error)); return direct(); }
  configured?.authorize?.();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-execution-scope-"));
  const marker = path.join(root, "admitted");
  const stdio = Array.isArray(options.stdio) ? [...options.stdio] : Array(3).fill(options.stdio ?? "pipe");
  const gateFd = stdio.length;
  stdio.push("pipe");
  let child: ChildProcess;
  try { child = spawn(executable, ["--user", "--scope", `--slice=${slice}`, "--quiet", "--collect", `--unit=${unit}`, "--expand-environment=no", "--",
    "/bin/sh", "-c", `printf admitted > "$1" || exit 125; IFS= read -r release <&${gateFd} || exit 125; exec ${gateFd}<&-; shift; ${legacyScope ? "" : "unset DBUS_SESSION_BUS_ADDRESS XDG_RUNTIME_DIR;"} exec "$@"`,
    "fabric-execution", marker, command, ...args], { ...options, ...(environment ? { env: environment } : {}), stdio }); }
  catch (error) { fs.rmSync(root, { recursive: true, force: true }); throw error; }
  const gate = child.stdio[gateFd] as Writable | null;
  gate?.on("error", () => { /* native close owns a broken gate */ });
  let closed = false, error: Error | undefined;
  const nativeClose = new Promise<void>(resolve => child.once("close", () => { closed = true; resolve(); }));
  child.once("error", value => { error = value; });
  let receipt: CgroupCustody | undefined;
  let pinAttempted = false;
  try {
    const birth = child.pid ? executionIdentity(child.pid) : undefined;
    await new Promise<void>((resolve, reject) => {
      let watcher: fs.FSWatcher | undefined, timer: ReturnType<typeof setTimeout> | undefined;
      let settled = false;
      const finish = (failure?: Error): void => {
        if (settled) return;
        settled = true;
        clearTimeout(timer); watcher?.close();
        child.removeListener("close", onClose); child.removeListener("error", onError);
        configured?.signal?.removeEventListener("abort", onAbort);
        if (failure) reject(failure); else resolve();
      };
      const onClose = (): void => finish(new ScopeAdmissionError("closed", { cause: error }));
      const onError = (cause: Error): void => finish(new ScopeAdmissionError("launcher", { cause }));
      const onAbort = (): void => finish(new ScopeAdmissionError("aborted", { cause: configured?.signal?.reason }));
      const check = (): void => {
        if (settled) return;
        if (closed) return onClose();
        if (configured?.signal?.aborted) return onAbort();
        try {
          if (directory && !receipt && birth && processScopePath(birth.pid) === directory) {
            pinAttempted = true;
            receipt = cgroupCustody(directory, birth);
            receipt.verify(birth); // pin + exact membership/owner BEFORE trusting the marker
          }
          if ((receipt || legacyScope) && fs.existsSync(marker)) {
            receipt?.verify(birth!); finish();
          }
        } catch (value) { finish(value instanceof Error ? value : new Error(String(value))); }
      };
      child.once("close", onClose); child.once("error", onError);
      configured?.signal?.addEventListener("abort", onAbort, { once: true });
      // Watch the parent before the initial read: creation can precede the read,
      // or follow it, without a lost wakeup. Events trigger checks, not authority.
      // The shell writes readiness only after systemd has placed it in the scope.
      try {
        timer = setTimeout(() => finish(new ScopeAdmissionError("timeout")), 5_000);
        watcher = fs.watch(root, check);
        watcher.once("error", cause => finish(new ScopeAdmissionError("watch", { cause })));
        // A terminal event during subscription must not strand the new watcher.
        if (settled) watcher.close(); else if (error) onError(error); else check();
      } catch (cause) { finish(new ScopeAdmissionError("watch", { cause })); }
    });
    if (receipt) executionCgroups.set(child, receipt);
    child.once("close", () => fs.rmSync(root, { recursive: true, force: true }));
    releases.set(child, () => { if (!closed) { receipt?.verify(birth!); gate?.end("release\n"); } });
    return child; // the spawn result carries custody; marker contents never do
  } catch (value) { error = value instanceof Error ? value : new Error(String(value)); }
  // The trusted admission shell exits on EOF, without execing an unowned target.
  // No ChildProcess.kill: that is numeric PID signalling on Node/Bun too.
  gate?.end();
  const joinLauncher = async (): Promise<void> => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([nativeClose, new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("Scope launcher termination is unconfirmed; refusing execution replay")), 2_000);
      })]);
    } finally { clearTimeout(timer); }
  };
  let markerPresent = false;
  try {
    try { await joinLauncher(); }
    catch (value) {
      if (!receipt) throw value;
      // EOF is the admission grace. A retained scope that does not drain still
      // escalates atomically, never by a numeric launcher PID or process group.
      await receipt.signal("SIGKILL");
      await joinLauncher();
    }
  } catch (value) {
    throw Object.assign(value instanceof Error ? value : new Error(String(value)), {
      launchOutcome: "unknown", cleanupPending: true, transport: "process", sessionId: String(child.pid),
    });
  } finally {
    // Marker existence can veto a replay, never grant custody or select a path.
    markerPresent = !!directory && fs.existsSync(marker);
    receipt?.dispose(); fs.rmSync(root, { recursive: true, force: true });
  }
  if (pinAttempted || markerPresent || (error instanceof ScopeAdmissionError && error.reason !== "timeout")) {
    throw error ?? new ScopeAdmissionError("timeout");
  }

  warning(error?.message ?? "systemd-run failed or scope admission timed out");
  return direct();
};
