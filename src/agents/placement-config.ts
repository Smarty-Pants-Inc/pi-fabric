import path from "node:path";
import fs from "node:fs";
import { AgentInputError, normalizeAgentCapabilityTokens } from "../host-compatibility.js";

export interface AgentPlacementConfig {
  /** Host-only shell-free argv templates. No placement is enabled when absent. */
  command: string[];
  default: "remote" | "local";
  /** Needs the selected target guarantees (empty is conservative); local is reserved. */
  capabilities: string[];
  /** Host-owned shared-root overrides; absent means no caller-local paths are portable. */
  sharedMounts?: string[];
  /** Host-only literal picker argv; fleet defaults to the installed sibling picker. */
  pickerCommand?: string[];
  /** Local/shared directory template containing result.md and the terminal rc marker. */
  resultDirectory?: string;
  /** Alternatively, prints JSON {rc:null} while pending, {rc,text,stderr?,worker?,sessionExport?} after native exit. */
  resultCommand?: string[];
  /** Host-only declaration that launch/result commands implement the native packet v1 contract. */
  workerProtocol?: "fabric-v1";
  /** Host-owned work-host name -> SSH alias mapping for post-launch {sshAlias}. */
  sshAliases?: Record<string, string>;
  /** Must request cancellation; only the terminal rc marker proves exit. */
  cancelCommand: string[];
  pollIntervalMs: number;
  commandTimeoutMs: number;
}

const placeholders = new Set(["id", "cwd", "task", "minutes", "model", "thinking", "host", "sshAlias", "resultDir"]);
const template = (value: unknown, key: string): string => {
  if (typeof value !== "string" || !value.trim() || value.includes("\0")) throw new Error(`Invalid agents.placement.${key}`);
  for (const match of value.matchAll(/\{([a-zA-Z]+)\}/g)) {
    if (!placeholders.has(match[1]!)) throw new Error(`Unknown agents.placement placeholder: ${match[1]}`);
  }
  return value;
};
const argv = (value: unknown, key: string): string[] => {
  if (!Array.isArray(value) || !value.length) throw new Error(`Invalid agents.placement.${key}: expected nonempty argv`);
  return value.map(entry => template(entry, key));
};
const bound = (value: unknown, fallback: number, min: number, max: number, key: string): number => {
  if (value === undefined) return fallback;
  if (typeof value !== "number" || !Number.isInteger(value) || value < min || value > max) throw new Error(`Invalid agents.placement.${key}`);
  return value;
};
export const normalizeAgentPlacement = (value: unknown): AgentPlacementConfig | undefined => {
  if (value === undefined) return undefined;
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Invalid agents.placement");
  const input = value as Record<string, unknown>;
  if (input.default !== undefined && input.default !== "local" && input.default !== "remote") throw new Error("Invalid agents.placement.default");
  const capabilities = normalizeAgentCapabilityTokens(input.capabilities ?? [], "agents.placement.capabilities")!;
  if (capabilities.includes("local")) throw new AgentInputError("agents.placement.capabilities", 'Invalid agents.placement.capabilities: "local" is a reserved need, never a launcher capability');
  if (input.workerProtocol !== undefined && input.workerProtocol !== "fabric-v1") throw new Error("Invalid agents.placement.workerProtocol");
  if (input.workerProtocol === "fabric-v1" && input.resultCommand === undefined) throw new Error("agents.placement.workerProtocol fabric-v1 requires resultCommand");
  if ((input.resultDirectory === undefined) === (input.resultCommand === undefined)) throw new Error("agents.placement requires exactly one of resultDirectory or resultCommand");
  if (input.resultDirectory !== undefined && (typeof input.resultDirectory !== "string" || !path.isAbsolute(input.resultDirectory))) throw new Error("agents.placement.resultDirectory must be absolute");
  let sharedMounts: string[] | undefined;
  if (input.sharedMounts !== undefined) {
    if (!Array.isArray(input.sharedMounts) || input.sharedMounts.length > 64) throw new Error("Invalid agents.placement.sharedMounts: expected absolute shared roots");
    sharedMounts = [...new Set(input.sharedMounts.map((root: unknown) => {
      if (typeof root !== "string" || !path.isAbsolute(root) || /[\u0000-\u001f\u007f-\u009f\u2028\u2029]/u.test(root) || /\{[a-zA-Z]+\}/u.test(root)) {
        throw new Error("Invalid agents.placement.sharedMounts: expected literal absolute shared roots");
      }
      const normalized = path.resolve(root);
      if (normalized === path.parse(normalized).root) throw new Error("Invalid agents.placement.sharedMounts: filesystem root is not a shared mount");
      return normalized;
    }))];
  }
  const pickerCommand = input.pickerCommand === undefined ? undefined : argv(input.pickerCommand, "pickerCommand");
  if (pickerCommand?.some(entry => /\{[a-zA-Z]+\}/u.test(entry))) throw new Error("agents.placement.pickerCommand requires literal argv, not placeholders");
  const command = argv(input.command, "command");
  if (command.some(entry => /\{(?:host|sshAlias|resultDir)\}/.test(entry))) throw new Error("agents.placement.command cannot use post-launch host/sshAlias/resultDir");
  let sshAliases: Record<string, string> | undefined;
  if (input.sshAliases !== undefined) {
    if (!input.sshAliases || typeof input.sshAliases !== "object" || Array.isArray(input.sshAliases)) throw new Error("Invalid agents.placement.sshAliases");
    sshAliases = {};
    for (const [host, alias] of Object.entries(input.sshAliases)) {
      if (!/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(host) || typeof alias !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(alias)) throw new Error("Invalid agents.placement.sshAliases mapping");
      Object.defineProperty(sshAliases, host, { value: alias, enumerable: true, configurable: true, writable: true });
    }
  }
  const usesAlias = [input.resultDirectory, ...(Array.isArray(input.resultCommand) ? input.resultCommand : []), ...(Array.isArray(input.cancelCommand) ? input.cancelCommand : [])]
    .some(entry => typeof entry === "string" && entry.includes("{sshAlias}"));
  if (usesAlias && !Object.keys(sshAliases ?? {}).length) throw new Error("agents.placement {sshAlias} requires sshAliases mapping");
  return {
    command, default: input.default ?? "local", capabilities,
    ...(sharedMounts ? { sharedMounts } : {}),
    ...(pickerCommand ? { pickerCommand } : {}),
    ...(sshAliases ? { sshAliases } : {}),
    ...(input.resultDirectory !== undefined ? { resultDirectory: template(input.resultDirectory, "resultDirectory") } : {}),
    ...(input.resultCommand !== undefined ? { resultCommand: argv(input.resultCommand, "resultCommand") } : {}),
    ...(input.workerProtocol === "fabric-v1" ? { workerProtocol: input.workerProtocol } : {}),
    cancelCommand: argv(input.cancelCommand, "cancelCommand"),
    pollIntervalMs: bound(input.pollIntervalMs, 1_000, 10, 60_000, "pollIntervalMs"),
    commandTimeoutMs: bound(input.commandTimeoutMs, 30_000, 100, 120_000, "commandTimeoutMs"),
  };
};

export interface AgentPlacementProbe {
  command: string;
  cwd: string;
  executable?: string;
  reason?: string;
}

// Keep filesystem-only readiness with the existing eager host config helpers:
// a separate module shared with the lazy transport would add a startup chunk.
// Session-only readiness, never serialized into host configuration.
const probes = new WeakMap<AgentPlacementConfig, AgentPlacementProbe>();
const executableFile = (candidate: string): boolean => {
  try {
    if (!fs.statSync(candidate).isFile()) return false;
    fs.accessSync(candidate, process.platform === "win32" ? fs.constants.F_OK : fs.constants.X_OK);
    return true;
  } catch { return false; }
};

/** Filesystem-only: no launcher, SSH, child process, optional adapter or timer. */
export const probeAgentPlacement = (config: AgentPlacementConfig, cwd: string): AgentPlacementProbe => {
  const command = config.command[0]!;
  const names = process.platform === "win32" && !path.extname(command)
    ? (process.env.PATHEXT ?? ".COM;.EXE;.BAT;.CMD").split(";").filter(Boolean).map(ext => command + ext.toLowerCase())
    : [command];
  const directories = path.isAbsolute(command) || command.includes("/") || command.includes("\\")
    ? [cwd]
    : (process.env.PATH ?? "").split(path.delimiter).map(entry => entry.trim().replace(/^"(.*)"$/, "$1")).filter(Boolean);
  const executable = /\{[a-zA-Z]+\}/.test(command) ? undefined
    : directories.flatMap(directory => names.map(name => path.resolve(directory, name))).find(executableFile);
  const probe: AgentPlacementProbe = {
    command, cwd,
    ...(executable ? { executable } : { reason: `placement-probe-failed: command missing or not executable: ${command}` }),
  };
  probes.set(config, probe);
  return probe;
};

export const agentPlacementProbe = (config: AgentPlacementConfig, cwd: string): AgentPlacementProbe => {
  const previous = probes.get(config);
  // A changed command or relative launch directory requires a new prelaunch check.
  return previous && previous.command === config.command[0] &&
    (path.isAbsolute(previous.command) || !/[\/\\]/.test(previous.command) || previous.cwd === cwd)
    ? previous : probeAgentPlacement(config, cwd);
};
