import path from "node:path";

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
  /** Must request cancellation; only the terminal rc marker proves exit. */
  cancelCommand: string[];
  pollIntervalMs: number;
  commandTimeoutMs: number;
}

const placeholders = new Set(["id", "cwd", "task", "minutes", "model", "thinking", "host", "resultDir"]);
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
  if (command.some(entry => entry.includes("{host}") || entry.includes("{resultDir}"))) throw new Error("agents.placement.command cannot use post-launch host/resultDir");
  return {
    command, default: input.default ?? "local", capabilities: [...new Set(capabilities as string[])],
    ...(input.resultDirectory !== undefined ? { resultDirectory: template(input.resultDirectory, "resultDirectory") } : {}),
    ...(input.resultCommand !== undefined ? { resultCommand: argv(input.resultCommand, "resultCommand") } : {}),
    cancelCommand: argv(input.cancelCommand, "cancelCommand"),
    pollIntervalMs: bound(input.pollIntervalMs, 1_000, 10, 60_000, "pollIntervalMs"),
    commandTimeoutMs: bound(input.commandTimeoutMs, 30_000, 100, 120_000, "commandTimeoutMs"),
  };
};
