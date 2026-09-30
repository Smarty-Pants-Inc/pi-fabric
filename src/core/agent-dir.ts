import fs from "node:fs";
import { homedir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

// Local mirror of `getAgentDir` from @earendil-works/pi-coding-agent (0.84.2).
// Kept identical so Fabric resolves the same config directory without
// importing the host package during extension load.

const ENV_AGENT_DIR = "PI_CODING_AGENT_DIR";
const CONFIG_DIR_NAME = ".pi";

const expandEnvDir = (envDir: string): string => {
  if (/^file:\/\//.test(envDir)) return fileURLToPath(envDir);
  if (envDir === "~") return homedir();
  if (envDir.startsWith("~/") || (process.platform === "win32" && envDir.startsWith("~\\"))) {
    return path.join(homedir(), envDir.slice(2));
  }
  return envDir;
};

export const resolveAgentDir = (): string => {
  const envDir = process.env[ENV_AGENT_DIR];
  if (envDir) return expandEnvDir(envDir);
  return path.join(homedir(), CONFIG_DIR_NAME, "agent");
};

// Shared with self-reload and spawn admission. Kept in this existing cheap core module so
// code splitting does not create an additional eager startup chunk for release selectors.
export const SELF_RELOAD_COMMAND = "fabric-release-reload";

const isFabricPackage = (root: string): boolean => {
  try {
    return (JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8")) as { name?: unknown }).name === "pi-fabric";
  } catch {
    return false;
  }
};

const real = (value: string): string => {
  try { return fs.realpathSync(value); } catch { return path.resolve(value); }
};

/** The Fabric package root this code loaded from: the nearest pi-fabric package.json above it. */
export const loadedFabricRoot = (moduleUrl: string): string | undefined => {
  let directory: string;
  try { directory = path.dirname(fileURLToPath(moduleUrl)); } catch { return undefined; }
  for (;;) {
    if (isFabricPackage(directory)) return real(directory);
    const parent = path.dirname(directory);
    if (parent === directory) return undefined;
    directory = parent;
  }
};

/** The local Fabric package the profile's settings.json activates, if exactly one. */
export const activeFabricRoot = (settingsPath: string): string | undefined => {
  let packages: unknown;
  try {
    packages = (JSON.parse(fs.readFileSync(settingsPath, "utf8")) as { packages?: unknown }).packages;
  } catch {
    return undefined;
  }
  if (!Array.isArray(packages)) return undefined;
  const base = path.dirname(settingsPath);
  const roots = packages.flatMap((entry) => {
    const source = typeof entry === "string" ? entry : (entry as { source?: unknown } | null)?.source;
    if (typeof source !== "string" || /^(npm|git|https?):/.test(source)) return [];
    const expanded = source.startsWith("~/") ? path.join(process.env.HOME ?? "", source.slice(2)) : source;
    const root = path.resolve(base, expanded);
    return isFabricPackage(root) ? [real(root)] : [];
  });
  return roots.length === 1 ? roots[0] : undefined;
};

export const releaseLabel = (root: string): string => path.basename(root);
