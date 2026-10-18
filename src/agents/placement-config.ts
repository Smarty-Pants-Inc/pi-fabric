import { diagnostics } from "../core/diagnostics.js";
import path from "node:path";
import fs from "node:fs";
import os from "node:os";
import { AgentInputError, normalizeAgentCapabilityTokens } from "../host-compatibility.js";

export type PlacementDeciderMode = "off" | "shadow" | "enforce";
export interface FabricPlacementDeciderConfig {
  /** Host-owned literal argv. No shell, interpolation, executable search or timeout knob.
   * Shadow requires an installed observer-only invocation; wire JSON cannot select it. */
  command: string[];
  /** Host-only smarty-launch-context argv; the caller PID is appended by Fabric, never accepted from a request. */
  contextCommand?: string[];
  mode: PlacementDeciderMode;
  /** One exact NATS subject token for the original owner's wake source, never a request pin. */
  wakeSourceMachine: string;
  /** Optional host-local launch-record directory; absent uses ~/.local/state/smarty/launches. */
  recordRoot?: string;
}
export type PlacementDeciderConfig = FabricPlacementDeciderConfig;
export const DEFAULT_PLACEMENT_DECIDER_MODE: PlacementDeciderMode = "off";

/** Filesystem-only native gate for placement enforce. Native user units and the trusted
 * caller resolver must both exist before a config can claim enforce semantics. */
export const nativePlacementPathAvailable = (contextCommand: readonly string[] | undefined, systemdRun = "/usr/bin/systemd-run", recordRoot?: string): boolean => {
  const executable = (file: string): boolean => {
    try { const stat = fs.statSync(file); return stat.isFile() && (process.platform === "win32" || (stat.mode & 0o111) !== 0); }
    catch { return false; }
  };
  const recordDirectoryAvailable = (): boolean => {
    let directory = path.resolve(recordRoot ?? path.join(os.homedir(), ".local", "state", "smarty", "launches"));
    try {
      const existing = fs.lstatSync(directory);
      return existing.isDirectory() && (existing.mode & 0o077) === 0 && fs.accessSync(directory, fs.constants.W_OK) === undefined;
    } catch { /* The first launch may create the leaf; find a writable existing ancestor. */ }
    while (directory !== path.dirname(directory)) {
      directory = path.dirname(directory);
      try {
        const existing = fs.lstatSync(directory);
        return existing.isDirectory() && fs.accessSync(directory, fs.constants.W_OK) === undefined;
      } catch { /* keep walking toward the root */ }
    }
    return false;
  };
  const context = contextCommand?.[0];
  return process.platform === "linux" && executable(systemdRun) && !!context &&
    path.basename(context) === "smarty-launch-context" && executable(context) && recordDirectoryAvailable();
};

const badPlacementDeciderField = (field: string): never => { throw new Error(`Invalid agents.placementDecider.${field}`); };

/** Host-only argv validation in the already-eager config helper; the adapter itself stays lazy. */
export const normalizePlacementDeciderConfig = (value: unknown): FabricPlacementDeciderConfig | undefined => {
  if (value === undefined) return undefined;
  if (!value || typeof value !== "object" || Array.isArray(value)) return badPlacementDeciderField("config");
  const input = value as Record<string, unknown>;
  for (const key of Object.keys(input)) if (!["command", "contextCommand", "mode", "wakeSourceMachine", "recordRoot"].includes(key)) badPlacementDeciderField(key);
  const mode = input.mode === undefined ? DEFAULT_PLACEMENT_DECIDER_MODE : input.mode;
  if (mode !== "off" && mode !== "shadow" && mode !== "enforce") return badPlacementDeciderField("mode");
  const validateArgv = (argv: unknown, field: "command" | "contextCommand", optional: boolean): string[] | undefined => {
    if (argv === undefined && optional) return undefined;
    if (!Array.isArray(argv) || argv.length < 1 || argv.length > 100 ||
        argv.some(arg => typeof arg !== "string" || /[\u0000-\u001f\u007f]/u.test(arg))) return badPlacementDeciderField(field);
    const entries = argv as string[];
    const executable = entries[0]!;
    if (!path.isAbsolute(executable) || !executable.trim() ||
        /^(?:sh|bash|dash|zsh|fish|ksh|csh|tcsh|cmd|powershell|pwsh)(?:\.exe)?$/iu.test(path.basename(executable)) ||
        /\.(?:cmd|bat)$/iu.test(executable)) return badPlacementDeciderField(field);
    try {
      if (!fs.statSync(executable).isFile()) return badPlacementDeciderField(field);
      fs.accessSync(executable, process.platform === "win32" ? fs.constants.F_OK : fs.constants.X_OK);
    } catch { return badPlacementDeciderField(field); }
    return [...entries];
  };
  const command = validateArgv(input.command, "command", false)!;
  const contextCommand = validateArgv(input.contextCommand, "contextCommand", true);
  if (typeof input.wakeSourceMachine !== "string" || !/^[a-z0-9][a-z0-9-]{0,31}$(?![\s\S])/u.test(input.wakeSourceMachine)) {
    return badPlacementDeciderField("wakeSourceMachine");
  }
  if (input.recordRoot !== undefined && (typeof input.recordRoot !== "string" || !path.isAbsolute(input.recordRoot))) return badPlacementDeciderField("recordRoot");
  return { command, ...(contextCommand ? { contextCommand } : {}), mode, wakeSourceMachine: input.wakeSourceMachine,
    ...(input.recordRoot ? { recordRoot: input.recordRoot } : {}) };
};

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
  /** Alternatively, prints JSON {rc:null} while pending, {rc,text,stderr?} after native exit. */
  resultCommand?: string[];
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
    cancelCommand: argv(input.cancelCommand, "cancelCommand"),
    pollIntervalMs: bound(input.pollIntervalMs, 1_000, 10, 60_000, "pollIntervalMs"),
    commandTimeoutMs: bound(input.commandTimeoutMs, 30_000, 100, 120_000, "commandTimeoutMs"),
  };
};

/** Placement-only, host-owned policy refresh at launch; no watchers or config migration. */
export const liveAgentPlacement = (file: string, initial?: AgentPlacementConfig): (() => AgentPlacementConfig | undefined) => {
  let current = initial;
  let stamp: string | undefined;
  let lastError: string | undefined;
  return () => {
    try {
      let next: string;
      try {
        const stat = fs.statSync(file);
        // Include identity and ctime/size for atomic replacement and coarse mtime filesystems.
        next = `${stat.dev}:${stat.ino}:${stat.mtimeMs}:${stat.ctimeMs}:${stat.size}`;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
        next = "missing";
      }
      if (next === stamp) return current;
      stamp = next; // Do not repeatedly parse/log an unchanged malformed edit.
      if (next === "missing") {
        current = undefined;
      } else {
        const document: unknown = JSON.parse(fs.readFileSync(file, "utf8"));
        if (!document || typeof document !== "object" || Array.isArray(document)) throw new Error("Invalid host Fabric configuration");
        const agents = (document as Record<string, unknown>).agents;
        if (agents !== undefined && (!agents || typeof agents !== "object" || Array.isArray(agents))) throw new Error("Invalid host agents configuration");
        current = normalizeAgentPlacement((agents as Record<string, unknown> | undefined)?.placement);
      }
      lastError = undefined;
    } catch (error) {
      const message = String(error);
      if (message !== lastError) diagnostics.warn("src/agents/placement-config.ts", `[pi-fabric] agents.placement live refresh failed (${file}); retaining last valid policy: ${message}`);
      lastError = message;
    }
    return current;
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
