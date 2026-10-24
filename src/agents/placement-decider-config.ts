import fs from "node:fs";
import path from "node:path";
import type { FabricPlacementDeciderConfig, PlacementDeciderMode } from "./placement-config.js";

export type { FabricPlacementDeciderConfig, PlacementDeciderConfig, PlacementDeciderMode } from "./placement-config.js";
export const DEFAULT_PLACEMENT_DECIDER_MODE: PlacementDeciderMode = "off";
const invalid = (field: string): never => { throw new Error(`Invalid agents.placementDecider.${field}`); };

/** Validate the host-owned helper only when placement admission is first used. */
export const normalizePlacementDeciderConfig = (value: unknown): FabricPlacementDeciderConfig | undefined => {
  if (value === undefined) return undefined;
  if (!value || typeof value !== "object" || Array.isArray(value)) return invalid("config");
  const input = value as Record<string, unknown>;
  for (const key of Object.keys(input)) if (!["command", "contextCommand", "mode", "wakeSourceMachine"].includes(key)) invalid(key);
  const mode = input.mode === undefined ? DEFAULT_PLACEMENT_DECIDER_MODE : input.mode;
  if (mode !== "off" && mode !== "shadow" && mode !== "enforce") return invalid("mode");
  const argv = (value: unknown, field: "command" | "contextCommand", optional: boolean): string[] | undefined => {
    if (value === undefined && optional) return undefined;
    if (!Array.isArray(value) || value.length < 1 || value.length > 100 || value.some(arg => typeof arg !== "string" || /[\u0000-\u001f\u007f]/u.test(arg))) return invalid(field);
    const entries = value as string[];
    const executable = entries[0]!;
    if (!path.isAbsolute(executable) || !executable.trim() || /^(?:sh|bash|dash|zsh|fish|ksh|csh|tcsh|cmd|powershell|pwsh)(?:\.exe)?$/iu.test(path.basename(executable)) || /\.(?:cmd|bat)$/iu.test(executable)) return invalid(field);
    try {
      if (!fs.statSync(executable).isFile()) return invalid(field);
      fs.accessSync(executable, process.platform === "win32" ? fs.constants.F_OK : fs.constants.X_OK);
    } catch { return invalid(field); }
    return [...entries];
  };
  const command = argv(input.command, "command", false)!;
  const contextCommand = argv(input.contextCommand, "contextCommand", true);
  if (typeof input.wakeSourceMachine !== "string" || !/^[a-z0-9][a-z0-9-]{0,31}$(?![\s\S])/u.test(input.wakeSourceMachine)) return invalid("wakeSourceMachine");
  return { command, ...(contextCommand ? { contextCommand } : {}), mode, wakeSourceMachine: input.wakeSourceMachine };
};
