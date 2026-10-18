import fs from "node:fs";
import { spawnSync } from "node:child_process";
import { childProcessEnvironment } from "../core/atomic-write.js";
import { LaunchRecordStore, processPidStart, readBootId } from "./launch-record.js";
import { nativePlacementEnforceAvailable, nativePlacementPathAvailable } from "../agents/placement-config.js";

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

export const systemdRunArguments = (unit: string, cwd: string, command: readonly string[], environment?: NodeJS.ProcessEnv): string[] => [
  "--user", "--unit", unit, "--collect", "--wait", "--pipe", "--quiet", "--working-directory", cwd,
  ...Object.keys(environment ?? {}).filter(key => /^[A-Za-z_][A-Za-z0-9_]*$/u.test(key) && environment?.[key] !== undefined)
    .map(key => `--setenv=${key}`),
  "--", ...command,
];

/** Recover a process interruption between the creating fence and systemd read-back.
 * A live, identity-bound unit closes started; absent or indeterminate evidence closes uncertain. */
export const verifyNativeLaunchRecord = (store: LaunchRecordStore, nativeLaunchId: string, systemctl = SYSTEMCTL) => {
  const record = store.get(nativeLaunchId);
  if (!record) throw new Error("Unknown launch record");
  const identity = systemdUnitIdentity(record.unit, systemctl);
  return store.verify(nativeLaunchId, record.generation, identity);
};

/** Native systemd paths are available for the existing shadow path; no capability is required. */
export const nativeResidentPlacementAvailable = (contextCommand: readonly string[] | undefined, run = SYSTEMD_RUN, recordRoot?: string): boolean =>
  nativePlacementPathAvailable(contextCommand, run, recordRoot);

/** Enforce additionally requires the context helper's verified-caller capability. */
export const nativeResidentPlacementEnforceAvailable = (contextCommand: readonly string[] | undefined, run = SYSTEMD_RUN, recordRoot?: string): boolean =>
  nativePlacementEnforceAvailable(contextCommand, run, recordRoot);
