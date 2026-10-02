import fs from "node:fs";
import os from "node:os";
import path from "node:path";

export const PI_TASK_RETRY_SETTINGS = { maxRetries: 6, baseDelayMs: 5_000, maxAgentDelayMs: 160_000 } as const;

/** Native Pi has no CLI retry-delay override. Give only this child its settings file. */
export const prepareRetryProfile = (cwd: string, directory: string, environment: NodeJS.ProcessEnv, scale = 1): string | undefined => {
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
  if (Object.hasOwn(global, "retry") || Object.hasOwn(project, "retry")) return undefined;
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  // Reuse the selected native profile's resources/state by reference. No auth
  // contents are read or copied, and the parent settings file is never changed.
  const entries = fs.existsSync(original) ? fs.readdirSync(original) : [];
  for (const name of new Set([...entries, "auth.json"])) {
    if (name === "settings.json" || name === "settings.json.lock") continue;
    const target = path.join(directory, name);
    if (!fs.existsSync(target) && !fs.lstatSync(target, { throwIfNoEntry: false })) {
      const source = path.join(original, name);
      const type = fs.statSync(source, { throwIfNoEntry: false })?.isDirectory() ? "junction" : "file";
      fs.symlinkSync(source, target, type);
    }
  }
  // Relative resource paths (including ../ siblings and exclusion prefixes)
  // retain their meaning in the original agent directory.
  for (const key of ["extensions", "skills", "prompts", "themes"]) {
    if (Array.isArray(global[key])) global[key] = global[key].map((value: unknown) => {
      if (typeof value !== "string") return value;
      const prefix = /^[!+-]/.test(value) ? value[0]! : "";
      const resource = value.slice(prefix.length);
      return prefix + (path.isAbsolute(resource) || resource.startsWith("~") ? resource : path.resolve(original, resource));
    });
  }
  if (Array.isArray(global.packages)) global.packages = global.packages.map((item: unknown) => {
    const absolute = (source: string) => /^(?:\.|\/)/.test(source) ? path.resolve(original, source) : source;
    if (typeof item === "string") return absolute(item);
    if (item && typeof item === "object" && "source" in item && typeof item.source === "string") return { ...item, source: absolute(item.source) };
    return item;
  });
  fs.writeFileSync(path.join(directory, "settings.json"), JSON.stringify({ ...global, retry: {
    ...PI_TASK_RETRY_SETTINGS, baseDelayMs: Math.max(1, Math.round(PI_TASK_RETRY_SETTINGS.baseDelayMs * scale)),
    maxAgentDelayMs: Math.max(1, Math.round(PI_TASK_RETRY_SETTINGS.maxAgentDelayMs * scale)),
  } }), { mode: 0o600 });
  return directory;
};
