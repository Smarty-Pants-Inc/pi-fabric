import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import type { ResidentHostConfig, ResidentHostOwner } from "./protocol.js";

export interface LegacyProcessIdentity {
  pid: number;
  startTime: string;
  commandLine: string;
  token: string;
}

let clockTicks: number | undefined;
const json = (file: string): Record<string, unknown> => JSON.parse(fs.readFileSync(file, "utf8"));
const stat = (pid: number): string[] => {
  const value = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
  return value.slice(value.lastIndexOf(")") + 2).trim().split(/\s+/);
};

/** Legacy has no handshake. Authenticate its immutable launch evidence, not mere PID liveness.
 * Unsupported platforms or incomplete evidence deliberately refuse automatic retirement.
 */
export const authenticateLegacyOwner = (
  config: ResidentHostConfig,
  owner: ResidentHostOwner,
): LegacyProcessIdentity | undefined => {
  if (process.platform !== "linux" || !Number.isSafeInteger(owner.pid) || owner.pid <= 1 || owner.pid === process.pid) return undefined;
  try {
    const current = json(path.join(config.residencyRoot, "owner.json"));
    const lock = json(path.join(config.residencyRoot, "host.lock"));
    if (!owner.token || current.pid !== owner.pid || current.token !== owner.token ||
      current.startedAt !== owner.startedAt || current.fabricExtensionPath ||
      lock.pid !== owner.pid || lock.token !== owner.token) return undefined;
    const fields = stat(owner.pid);
    if (["Z", "X"].includes(fields[0]!)) return undefined;
    const startTime = fields[19];
    if (!startTime || !/^\d+$/.test(startTime)) return undefined;
    const configPath = path.join(config.residencyRoot, "config.json");
    const environment = fs.readFileSync(`/proc/${owner.pid}/environ`, "utf8").split("\0");
    if (!environment.includes(`PI_FABRIC_RESIDENT_CONFIG=${configPath}`)) return undefined;
    const commandLine = fs.readFileSync(`/proc/${owner.pid}/cmdline`, "utf8");
    const args = commandLine.split("\0").filter(Boolean);
    // Native Pi sets process.title to pi, overwriting argv. Otherwise require the RPC entry.
    if (!(args.length === 1 && args[0] === "pi") &&
      !(args.includes("--mode") && args.includes("rpc") && args.includes("--extension") &&
        args.some(arg => arg.endsWith("/residency/pi-entry.js")))) return undefined;
    const tracePath = path.join(config.residencyRoot, "launcher.log");
    if (fs.statSync(tracePath).size > 1024 * 1024) return undefined;
    const launches = fs.readFileSync(tracePath, "utf8").trim().split("\n")
      .map(line => JSON.parse(line) as { event?: string; pid?: number; at?: number; configPath?: string });
    const child = launches.slice().reverse().find(row => row.event === "child-spawned" && row.pid === owner.pid);
    const launcher = launches.slice().reverse().find(row => row.event === "launcher-started" && row.pid === Number(fields[1]) && row.configPath === configPath);
    if (!child?.at || !launcher?.at || child.at < launcher.at || owner.startedAt < child.at) return undefined;
    const parentArgs = fs.readFileSync(`/proc/${Number(fields[1])}/cmdline`, "utf8").split("\0");
    if (!parentArgs.includes(configPath) || !parentArgs.includes("--config") ||
      !parentArgs.some(arg => arg.endsWith("/residency/launcher.js"))) return undefined;
    clockTicks ??= Number(execFileSync("getconf", ["CLK_TCK"], { encoding: "utf8", timeout: 1000 }));
    if (!Number.isFinite(clockTicks) || clockTicks <= 0) return undefined;
    const uptime = Number(fs.readFileSync("/proc/uptime", "utf8").split(" ")[0]);
    const startedAt = Date.now() - uptime * 1000 + Number(startTime) * 1000 / clockTicks;
    // child-spawned is written just after spawn, not after Pi's potentially slow ready hook.
    if (Math.abs(startedAt - child.at) > 1000) return undefined;
    return { pid: owner.pid, startTime, commandLine, token: owner.token };
  } catch { return undefined; }
};

export const legacyProcessStopped = (identity: LegacyProcessIdentity): boolean => {
  try {
    const fields = stat(identity.pid);
    return fields[19] === identity.startTime && ["T", "t"].includes(fields[0]!);
  } catch { return false; }
};

/** Reauthenticate all four identity fields immediately before EACH signal, including resume. */
export const signalLegacyOwner = (
  config: ResidentHostConfig,
  owner: ResidentHostOwner,
  identity: LegacyProcessIdentity,
  signal: NodeJS.Signals,
): boolean => {
  const current = authenticateLegacyOwner(config, owner);
  if (!current || current.pid !== identity.pid || current.startTime !== identity.startTime ||
    current.commandLine !== identity.commandLine || current.token !== identity.token) return false;
  try { process.kill(identity.pid, signal); return true; } catch { return false; }
};
