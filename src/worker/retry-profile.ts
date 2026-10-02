import fs from "node:fs";
import os from "node:os";
import path from "node:path";

export const PI_TASK_RETRY_SETTINGS = { maxRetries: 6, baseDelayMs: 5_000, maxAgentDelayMs: 160_000 } as const;
export const taskRetrySettings = (scale = 1) => ({
  ...PI_TASK_RETRY_SETTINGS,
  baseDelayMs: Math.max(1, Math.round(PI_TASK_RETRY_SETTINGS.baseDelayMs * scale)),
  maxAgentDelayMs: Math.max(1, Math.round(PI_TASK_RETRY_SETTINGS.maxAgentDelayMs * scale)),
});

/** Select the canonical profile; never alias auth.json (Pi locks with realpath:false).
 * Retry defaults are applied in memory by the native SDK task entry, not saved.
 */
export const prepareRetryProfile = (cwd: string, _directory: string, environment: NodeJS.ProcessEnv, _scale = 1): string | undefined => {
  const original = path.resolve(environment.PI_CODING_AGENT_DIR?.replace(/^~(?=\/|$)/, os.homedir()) ?? path.join(os.homedir(), ".pi", "agent"));
  const settings = (file: string): Record<string, unknown> => {
    try { return JSON.parse(fs.readFileSync(file, "utf8").replace(/^\uFEFF/, "")) as Record<string, unknown>; }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return {};
      throw error; // Never overwrite or mask unreadable/malformed user settings.
    }
  };
  const global = settings(path.join(original, "settings.json"));
  const project = settings(path.join(cwd, ".pi", "settings.json"));
  return Object.hasOwn(global, "retry") || Object.hasOwn(project, "retry") ? undefined : original;
};

/** Use only the SDK belonging to the selected native launcher, never our peer copy.
 * Opaque/custom launchers keep their native settings and Fabric's resume budget.
 */
export const resolveRetrySdk = (binary: string): string | undefined => {
  try {
    const entry = fs.realpathSync(binary);
    if (path.basename(entry) !== "cli.js") return undefined; // Do not bypass custom/native launcher setup.
    let directory = path.dirname(entry);
    while (true) {
      const manifest = path.join(directory, "package.json");
      if (fs.existsSync(manifest)) {
        const pkg = JSON.parse(fs.readFileSync(manifest, "utf8"));
        const dist = path.join(directory, "dist");
        const [major, minor] = String(pkg.version).split(".").map(Number);
        const files = ["index.js", "main.js", "cli/args.js", "cli/project-trust.js", "core/project-trust.js", "core/http-dispatcher.js", "extensions/index.js"];
        if (pkg.name === "@earendil-works/pi-coding-agent" &&
            (major! > 0 || (major === 0 && minor! >= 87)) && files.every(file => fs.existsSync(path.join(dist, file)))) {
          return dist;
        }
        return undefined;
      }
      const parent = path.dirname(directory);
      if (parent === directory) return undefined;
      directory = parent;
    }
  } catch { return undefined; }
};
