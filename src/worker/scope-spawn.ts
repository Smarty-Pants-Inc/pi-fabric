import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import type { ChildProcess, SpawnOptions } from "node:child_process";
import type { Writable } from "node:stream";
import { findExecutable } from "../agents/transports/process-utils.js";
import { cgroupCustody, executionCgroups, executionIdentity, processScopePath, scopePath, scopeLauncherEnvironment } from "../process-cgroup.js";

let warned = false;
const warn = (reason: string): void => {
  if (warned) return;
  warned = true;
  console.warn(`[pi-fabric] Linux execution scope: ${reason}; using legacy process-group custody`);
};
const releases = new WeakMap<ChildProcess, () => void>();
/** Release only after native-close and stream listeners and custody are installed. */
export const releaseScopedChild = (child: ChildProcess): void => { releases.get(child)?.(); releases.delete(child); };

export const spawnScopedExecution = async (
  spawn: (command: string, args: readonly string[], options: SpawnOptions) => ChildProcess,
  command: string, args: readonly string[], options: SpawnOptions,
  parentCanRetainScopes = true,
): Promise<ChildProcess> => {
  if (process.platform !== "linux") return spawn(command, args, options);
  // Only the launcher needs the user bus. Executions have no bus dependency;
  // XDG_RUNTIME_DIR must go too: systemd-run otherwise discovers the same bus.
  const env = { ...(options.env ?? process.env) };
  delete env.DBUS_SESSION_BUS_ADDRESS;
  delete env.XDG_RUNTIME_DIR;
  const targetOptions = { ...options, env };
  if (!parentCanRetainScopes) { warn("parent lacks cgroup custody capability"); return spawn(command, args, targetOptions); }
  const executable = findExecutable("systemd-run");
  if (!executable) { warn("systemd-run unavailable"); return spawn(command, args, targetOptions); }
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-execution-scope-"));
  const marker = path.join(root, "admitted");
  const stdio = Array.isArray(options.stdio) ? [...options.stdio] : Array(3).fill(options.stdio ?? "pipe");
  const gateFd = stdio.length;
  stdio.push("pipe");
  const unit = `fabric-execution-${randomUUID()}.scope`;
  const child = spawn(executable, ["--user", "--scope", `--unit=${unit}`, "--expand-environment=no", "--quiet", "--collect", "--",
    "/bin/sh", "-c", `/bin/cat /proc/self/cgroup > "$1.tmp" && /bin/mv "$1.tmp" "$1" || exit 125; IFS= read -r release <&${gateFd} || exit 125; exec ${gateFd}<&-; shift; unset DBUS_SESSION_BUS_ADDRESS XDG_RUNTIME_DIR; exec "$@"`,
    "fabric-execution", marker, command, ...args], { ...options, env: scopeLauncherEnvironment(options.env), stdio });
  const gate = child.stdio[gateFd] as Writable | null;
  gate?.on("error", () => { /* native close owns a broken admission pipe */ });
  let closed = false, error: Error | undefined;
  const nativeClose = new Promise<void>(resolve => child.once("close", () => { closed = true; resolve(); }));
  child.once("error", value => { error = value; });
  const deadline = Date.now() + 5_000;
  while (!fs.existsSync(marker) && !closed && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 10));
  if (fs.existsSync(marker)) {
    try {
      const directory = (child.pid ? processScopePath(child.pid, unit) : undefined) ?? scopePath(fs.readFileSync(marker, "utf8"), unit);
      if (directory) { fs.statSync(directory); executionCgroups.set(child, cgroupCustody(directory, child.pid ? executionIdentity(child.pid) : undefined)); }
      else warn("admitted scope has no cgroup v2 path");
    } catch (value) { warn(`admitted cgroup unavailable: ${String(value)}`); }
    const cleanup = (): void => { fs.rmSync(root, { recursive: true, force: true }); };
    child.once("close", cleanup);
    // A pipe, not a polling file gate: worker death before handoff produces EOF
    // and exits the launcher WITHOUT ever execing an unowned target.
    releases.set(child, () => { if (!closed) gate?.end("release\n"); });
    return child; // admitted commands are NEVER replayed
  }
  if (!closed) {
    // No admission marker means the target was never exec'd. Join the captured
    // native launcher before permitting a fallback (never numeric pid cleanup).
    child.kill("SIGKILL");
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([nativeClose, new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("Scope launcher exit unconfirmed; refusing execution replay")), 2_000);
      })]);
    } finally { clearTimeout(timer); }
  }
  fs.rmSync(root, { recursive: true, force: true });
  warn(error?.message ?? "systemd-run failed or user manager unavailable");
  return spawn(command, args, targetOptions);
};
