import fs from "node:fs";
import { spawnSync } from "node:child_process";
import { childProcessEnvironment } from "../core/atomic-write.js";
import { processPidStart, readBootId } from "./launch-record.js";

export const SYSTEMD_RUN = "/usr/bin/systemd-run";
export const SYSTEMCTL = "/usr/bin/systemctl";

export interface NativeUnitIdentity {
  boot_id: string;
  invocation_id: string;
  main_pid_start: string;
}

export const systemdRunAvailable = (run = SYSTEMD_RUN): boolean => {
  try { return process.platform === "linux" && fs.statSync(run).isFile() && (fs.statSync(run).mode & 0o111) !== 0; }
  catch { return false; }
};

export const systemdUnitIdentity = (unit: string, systemctl = SYSTEMCTL): NativeUnitIdentity | undefined => {
  if (process.platform !== "linux" || !readBootId()) return undefined;
  let result;
  try {
    result = spawnSync(systemctl, ["--user", "show", unit, "--property=InvocationID", "--property=MainPID", "--no-pager"], {
      env: { ...childProcessEnvironment(), SYSTEMD_COLORS: "0", SYSTEMD_PAGER: "cat" }, encoding: "utf8", timeout: 1_000,
    });
  } catch { return undefined; }
  if (result.error || result.status !== 0 || typeof result.stdout !== "string") return undefined;
  let invocation: string | undefined;
  let pid: number | undefined;
  for (const line of result.stdout.split(/\r?\n/u)) {
    const [key, value] = line.split("=", 2);
    if (key === "InvocationID" && value) invocation = value.trim();
    if (key === "MainPID" && value && /^\d+$/u.test(value.trim())) pid = Number(value.trim());
  }
  const boot = readBootId();
  const start = pid ? processPidStart(pid) : null;
  if (!boot || !invocation || !/^[0-9a-f]{32}$/u.test(invocation) || !start) return undefined;
  return { boot_id: boot, invocation_id: invocation, main_pid_start: start };
};

export const systemdRunArguments = (unit: string, cwd: string, command: readonly string[]): string[] => [
  "--user", "--unit", unit, "--collect", "--wait", "--pipe", "--quiet", "--working-directory", cwd, "--", ...command,
];

/** A native systemd user unit is mandatory for enforce; this predicate is deliberately filesystem-only. */
export const nativeResidentPlacementAvailable = (contextCommand: readonly string[] | undefined, run = SYSTEMD_RUN): boolean =>
  process.platform === "linux" && systemdRunAvailable(run) && !!contextCommand?.length &&
  contextCommand[0]!.split(/[\\/]/u).at(-1) === "smarty-launch-context" &&
  (() => { try { return fs.statSync(contextCommand[0]!).isFile() && (fs.statSync(contextCommand[0]!).mode & 0o111) !== 0; } catch { return false; } })();