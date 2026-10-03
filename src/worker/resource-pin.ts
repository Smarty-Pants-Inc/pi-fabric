import fs from "node:fs";
import { pathToFileURL } from "node:url";
import type { DefaultPackageManager, ResolvedPaths } from "@earendil-works/pi-coding-agent";
import { loadedFabricRoot } from "../core/agent-dir.js";

/** Install before native Pi resolves resources, including its project-trust bootstrap.
 * Dedicated child process only: decorate public discovery methods on the supplied
 * launcher's SDK, not a peer SDK or private loader fields. Every reload/cwd switch
 * re-runs normal discovery/trust, then removes other Fabric generations BEFORE
 * loading code. Other packages, profile extensions and resource metadata survive.
 * Filtering a post-load extensionsOverride would be too late: factories already ran.
 */
export const installFabricResourcePin = (PackageManager: typeof DefaultPackageManager, extensionPath: string): (() => void) => {
  const rootOf = (file: string) => {
    try { return loadedFabricRoot(pathToFileURL(fs.realpathSync(file)).href); }
    catch { return loadedFabricRoot(pathToFileURL(file).href); }
  };
  const pinnedRoot = rootOf(extensionPath);
  if (!pinnedRoot) throw new Error(`Pinned extension is not a Fabric release: ${extensionPath}`);
  const filter = (paths: ResolvedPaths): ResolvedPaths => {
    const keep = (resource: { path: string }) => {
      const root = rootOf(resource.path);
      return !root || root === pinnedRoot;
    };
    return { extensions: paths.extensions.filter(keep), skills: paths.skills.filter(keep),
      prompts: paths.prompts.filter(keep), themes: paths.themes.filter(keep) };
  };
  const prototype = PackageManager.prototype;
  const resolve = prototype.resolve;
  const resolveExtensionSources = prototype.resolveExtensionSources;
  prototype.resolve = async function (...args) { return filter(await resolve.apply(this, args)); };
  prototype.resolveExtensionSources = async function (...args) { return filter(await resolveExtensionSources.apply(this, args)); };
  return () => { prototype.resolve = resolve; prototype.resolveExtensionSources = resolveExtensionSources; };
};
