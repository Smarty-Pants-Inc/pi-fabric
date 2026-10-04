import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { loadedFabricRoot } from "./agent-dir.js";

type ResourceType = "extensions" | "skills" | "prompts" | "themes";
const canonical = (file: string): string => {
  try { return fs.realpathSync(file); } catch { return path.resolve(file); }
};

/** Package ancestry is not resource identity: a Fabric checkout can also contain
 * authorized caller/project hooks and unrelated packages. Match the release's
 * declared (or conventional) resources instead, including direct -e paths and
 * symlinks, without importing Pi or a glob engine into the launch boundary.
 */
export const fabricResourceRoot = (file: string, type: ResourceType): string | undefined => {
  const resource = canonical(file);
  const root = loadedFabricRoot(pathToFileURL(resource).href);
  if (!root) return undefined;
  // The built and source Fabric entrypoints remain Fabric even if a manifest
  // only advertises the other one. Internal worker hooks are NOT entrypoints.
  if (type === "extensions" && ["dist/index.js", "src/index.ts", "src/index.js"].some(entry => resource === canonical(path.join(root, entry)))) return root;
  let entries: unknown;
  try {
    const manifest = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8")) as { pi?: Partial<Record<ResourceType, unknown>> };
    entries = manifest.pi ? manifest.pi[type] ?? [] : [type];
  } catch { return undefined; }
  const matches = (entry: string): boolean => {
    const target = path.resolve(root, entry);
    if (path.matchesGlob(resource, target)) return true;
    const resolved = canonical(target);
    if (resource === resolved) return true;
    // Manifest entries may name resource directories as well as files/globs.
    try {
      const relative = path.relative(resolved, resource);
      return fs.statSync(resolved).isDirectory() && !relative.startsWith(`..${path.sep}`) && relative !== ".." && !path.isAbsolute(relative);
    } catch { return false; }
  };
  const patterns = Array.isArray(entries) ? entries.filter((entry): entry is string => typeof entry === "string") : [];
  return patterns.some(entry => !entry.startsWith("!") && matches(entry)) &&
    !patterns.some(entry => entry.startsWith("!") && matches(entry.slice(1))) ? root : undefined;
};
