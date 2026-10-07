import fs from "node:fs";
import { formatWithOptions } from "node:util";
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

export const resolveAgentDir = (envDir = process.env[ENV_AGENT_DIR]): string => {
  if (envDir) return expandEnvDir(envDir);
  return path.join(homedir(), CONFIG_DIR_NAME, "agent");
};

// Shared with self-reload and the release census. Kept in this existing cheap core module so
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

const LOG_CAP_BYTES = 5 * 1024 * 1024;
const NOTICE_WINDOW_MS = 10 * 60 * 1000;
type DiagnosticContext = {
  hasUI: boolean;
  mode?: string;
  ui: { notify(message: string, level: "warning"): void };
};
let interactive: DiagnosticContext["ui"] | undefined;
const notices = new Map<string, number>();

/** Bind the supplied host UI before bootstrap. RPC has UI APIs, but no terminal TUI.
 * Keep the binding through shutdown so late background settlements cannot paint stderr.
 * The next session replaces it (including switching back to headless mode).
 */
export const configureFabricDiagnostics = (context?: DiagnosticContext): void => {
  interactive = context?.hasUI && (context.mode === undefined || context.mode === "tui" || context.mode === "interactive")
    ? context.ui : undefined;
};

/** No host imports, timers, I/O or optional engines until an interactive warning occurs.
 * Returns true when the bound terminal UI owns the diagnostic (logged and, unless
 * deduplicated, notified); callers then must not notify it again. False means it went
 * to console.warn, so a caller's explicit UI notice is still the only user-visible one.
 */
export const fabricWarn = (...args: unknown[]): boolean => {
  if (!interactive) {
    console.warn(...args);
    return false;
  }
  let message: string;
  try {
    message = formatWithOptions({ colors: false }, ...args);
  } catch {
    message = "[pi-fabric] Diagnostic arguments could not be formatted";
  }
  const logPath = process.env.PI_FABRIC_LOG || path.join(resolveAgentDir(), "fabric", "logs", "diagnostics.log");
  let logFailed = false;
  try {
    fs.mkdirSync(path.dirname(logPath), { recursive: true, mode: 0o700 });
    // One physical line per diagnostic, retaining stacks and multiline messages as escapes.
    const line = `${new Date().toISOString()} ${message.replaceAll("\r", "\\r").replaceAll("\n", "\\n")}\n`;
    let size = 0;
    try { size = fs.statSync(logPath).size; }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    if (size && size + Buffer.byteLength(line) > LOG_CAP_BYTES) {
      // Remove only the previous rotation, never a directory or unrelated state.
      try { fs.unlinkSync(`${logPath}.1`); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
      fs.renameSync(logPath, `${logPath}.1`);
    }
    fs.appendFileSync(logPath, line, { mode: 0o600 });
  } catch {
    // Disk failure is not permission to paint over the editor or alter caller retry logic.
    logFailed = true;
  }
  // Randomized delays, holders, attempts and labels must not create a notice per retry.
  const key = /mesh lock timeout|FABRIC_MESH_LOCK_TIMEOUT/.test(message) ? "mesh-lock-timeout" : message;
  const now = Date.now();
  const last = notices.get(key);
  if (last !== undefined && now - last < NOTICE_WINDOW_MS) return true;
  for (const [oldKey, at] of notices) if (now - at >= NOTICE_WINDOW_MS) notices.delete(oldKey);
  // Bound memory without evicting live dedup keys (which would re-enable a notice storm).
  if (notices.size >= 1024) return true;
  notices.set(key, now);
  try {
    interactive.notify(`${key === "mesh-lock-timeout" ? "[pi-fabric] Mesh lock contention; background operations are retrying." : message.split("\n")[0]} ${logFailed ? "Diagnostic log unavailable:" : "Details:"} ${logPath}`, "warning");
  } catch { /* A stale UI must not change operation or retry semantics. */ }
  return true;
};
