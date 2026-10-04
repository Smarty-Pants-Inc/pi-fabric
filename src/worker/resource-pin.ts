import type { DefaultPackageManager, ResolvedPaths } from "@earendil-works/pi-coding-agent";
import { fabricResourceRoot } from "../core/fabric-resource.js";

/** Install before native Pi resolves resources, including its project-trust bootstrap.
 * Dedicated child process only: decorate public discovery methods on the supplied
 * launcher's SDK, not a peer SDK or private loader fields. Every reload/cwd switch
 * re-runs normal discovery/trust, then removes other Fabric generations BEFORE
 * loading code. Other packages, profile extensions and resource metadata survive.
 * Filtering a post-load extensionsOverride would be too late: factories already ran.
 */
export const installFabricResourcePin = (PackageManager: typeof DefaultPackageManager, extensionPath: string): (() => void) => {
  const pinnedRoot = fabricResourceRoot(extensionPath, "extensions");
  if (!pinnedRoot) throw new Error(`Pinned extension is not a Fabric release: ${extensionPath}`);
  const filter = (paths: ResolvedPaths): ResolvedPaths => {
    const keep = (type: "extensions" | "skills" | "prompts" | "themes") => (resource: { path: string }) => {
      const root = fabricResourceRoot(resource.path, type);
      return !root || root === pinnedRoot;
    };
    return { extensions: paths.extensions.filter(keep("extensions")), skills: paths.skills.filter(keep("skills")),
      prompts: paths.prompts.filter(keep("prompts")), themes: paths.themes.filter(keep("themes")) };
  };
  const prototype = PackageManager.prototype;
  const resolve = prototype.resolve;
  const resolveExtensionSources = prototype.resolveExtensionSources;
  prototype.resolve = async function (...args) { return filter(await resolve.apply(this, args)); };
  prototype.resolveExtensionSources = async function (...args) { return filter(await resolveExtensionSources.apply(this, args)); };
  return () => { prototype.resolve = resolve; prototype.resolveExtensionSources = resolveExtensionSources; };
};
