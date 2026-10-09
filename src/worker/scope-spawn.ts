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
export type ScopeLaunch = { executable: string; slice: string; prefix?: "worker" | "execution"; warn: (reason: string) => void; authorize?: () => void };
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
  const environment = scopeLauncherEnvironment(options.env);
  let directory: string;
  try {
    // A spawn placement, not a path sampled from a marker (or a migrated PID).
    const systemctl = findExecutable("systemctl");
    if (!systemctl || !fs.existsSync("/sys/fs/cgroup/cgroup.controllers")) throw new Error("cgroup v2 user manager unavailable");
    const { stdout } = await executeFile(systemctl, ["--user", "show", "--property=ControlGroup", "--value", "--", "-.slice"], { env: environment, timeoutMs: 2_000 });
    directory = scopeDirectory(stdout.trim(), slice, unit);
  } catch (error) { warning(String(error)); return direct(); }
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-execution-scope-"));
  const marker = path.join(root, "admitted");
  const stdio = Array.isArray(options.stdio) ? [...options.stdio] : Array(3).fill(options.stdio ?? "pipe");
  const gateFd = stdio.length;
  stdio.push("pipe");
  configured?.authorize?.();
  const child = spawn(executable, ["--user", "--scope", `--slice=${slice}`, "--quiet", "--collect", `--unit=${unit}`, "--expand-environment=no", "--",
    "/bin/sh", "-c", `printf admitted > "$1" || exit 125; IFS= read -r release <&${gateFd} || exit 125; exec ${gateFd}<&-; shift; unset DBUS_SESSION_BUS_ADDRESS XDG_RUNTIME_DIR; exec "$@"`,
    "fabric-execution", marker, command, ...args], { ...options, env: environment, stdio });
  const gate = child.stdio[gateFd] as Writable | null;
  gate?.on("error", () => { /* native close owns a broken gate */ });
  let closed = false, error: Error | undefined;
  const nativeClose = new Promise<void>(resolve => child.once("close", () => { closed = true; resolve(); }));
  child.once("error", value => { error = value; });
  const birth = child.pid ? executionIdentity(child.pid) : undefined;
  let receipt: CgroupCustody | undefined;
  const deadline = Date.now() + 5_000;
  try {
    while (!closed && Date.now() < deadline) {
      if (!receipt && birth && processScopePath(birth.pid) === directory) {
        receipt = cgroupCustody(directory, birth);
        receipt.verify(birth); // pin + exact membership/owner BEFORE trusting the marker
      }
      if (receipt && fs.existsSync(marker)) {
        receipt.verify(birth!);
        executionCgroups.set(child, receipt);
        child.once("close", () => fs.rmSync(root, { recursive: true, force: true }));
        releases.set(child, () => { if (!closed) { receipt!.verify(birth!); gate?.end("release\n"); } });
        return child; // the spawn result carries custody; marker contents never do
      }
      await new Promise(resolve => setTimeout(resolve, 10));
    }
  } catch (value) { error = value instanceof Error ? value : new Error(String(value)); }
  // The trusted admission shell exits on EOF, without execing an unowned target.
  // No ChildProcess.kill: that is numeric PID signalling on Node/Bun too.
  gate?.end();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([nativeClose, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error("Scope launcher termination is unconfirmed; refusing execution replay")), 2_000);
    })]);
  } finally { clearTimeout(timer); receipt?.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
  // A verified scope that subsequently lost exact ownership must not downgrade.
  if (receipt || (error && error.message.includes("ownership")) || (error && error.message.includes("spawn scope"))) throw error ?? new Error("Scope admission failed");
  warning(error?.message ?? "systemd-run failed or scope admission timed out");
  return direct();
};
