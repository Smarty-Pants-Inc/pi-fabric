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
  const deadline = Date.now() + 5_000;
  try {
    const birth = child.pid ? executionIdentity(child.pid) : undefined;
    while (!closed && Date.now() < deadline && !configured?.signal?.aborted) {
      if (directory && !receipt && birth && processScopePath(birth.pid) === directory) {
        pinAttempted = true;
        receipt = cgroupCustody(directory, birth);
        receipt.verify(birth); // pin + exact membership/owner BEFORE trusting the marker
      }
      if ((receipt || legacyScope) && fs.existsSync(marker)) {
        receipt?.verify(birth!);
        if (receipt) executionCgroups.set(child, receipt);
        child.once("close", () => fs.rmSync(root, { recursive: true, force: true }));
        releases.set(child, () => { if (!closed) { receipt?.verify(birth!); gate?.end("release\n"); } });
        return child; // the spawn result carries custody; marker contents never do
      }
      await new Promise(resolve => setTimeout(resolve, 10));
    }
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
  if (pinAttempted || markerPresent) throw error ?? new Error("Launcher never confirmed its spawn scope; refusing execution replay");

  warning(error?.message ?? "systemd-run failed or scope admission timed out");
  return direct();
};
