import { accessSync, constants, realpathSync } from "node:fs";
import { homedir } from "node:os";
import path from "node:path";
import { findExecutable } from "./transports/process-utils.js";

export interface PiBinaryResolutionOptions {
  env?: NodeJS.ProcessEnv;
  homeDirectory?: string;
  isExecutable?: (file: string) => boolean;
}

const executable = (file: string): boolean => {
  try {
    accessSync(file, constants.X_OK);
    return true;
  } catch {
    return false;
  }
};

export const resolvePiBinary = (
  configured?: string,
  options: PiBinaryResolutionOptions = {},
): string => {
  const env = options.env ?? process.env;
  const pin = (selected: string): string => {
    // The same argv pathname is not the same artifact: a fleet `pi` symlink
    // may move between attempts, or a bare configured command may resolve on
    // the transport's PATH rather than the owner's. Resolve once for this manager
    // and hand the physical selection to every first launch, retry and child.
    const located = path.isAbsolute(selected) || selected.includes("/") || selected.includes("\\")
      ? selected : findExecutable(selected, env, options.isExecutable) ?? selected;
    try { return realpathSync(located); } catch { return located; }
  };
  if (configured !== undefined) return pin(configured);
  if (env.PI_FABRIC_PI_BINARY !== undefined) return pin(env.PI_FABRIC_PI_BINARY);

  if (env.LOCALTERM === "1") {
    const shim = path.join(options.homeDirectory ?? homedir(), ".localterm", "shims", "pi");
    if ((options.isExecutable ?? executable)(shim)) return pin(shim);
  }

  // Resolve with the host's PATH now: Herdr workers inherit the server's
  // environment, whose PATH need not contain the owner's Pi launcher.
  return pin("pi");
};
