import path from "node:path";
import fs from "node:fs";

export interface AgentPlacementConfig {
  /** Host-only shell-free argv templates. No placement is enabled when absent. */
  command: string[];
  default: "remote" | "local";
  /** Needs the selected target guarantees (empty is conservative). */
  capabilities: string[];
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
  const capabilities = input.capabilities ?? [];
  if (!Array.isArray(capabilities) || !capabilities.every(entry => typeof entry === "string" && !!entry.trim())) throw new Error("Invalid agents.placement.capabilities");
  if ((input.resultDirectory === undefined) === (input.resultCommand === undefined)) throw new Error("agents.placement requires exactly one of resultDirectory or resultCommand");
  if (input.resultDirectory !== undefined && (typeof input.resultDirectory !== "string" || !path.isAbsolute(input.resultDirectory))) throw new Error("agents.placement.resultDirectory must be absolute");
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
    command, default: input.default ?? "local", capabilities: [...new Set(capabilities as string[])],
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
      if (message !== lastError) console.warn(`[pi-fabric] agents.placement live refresh failed (${file}); retaining last valid policy: ${message}`);
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
