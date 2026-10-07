import fs from "node:fs";
import path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const EXTENSION_FLAGS = new Set(["-e", "--extension"]);

type FabricPackage = { root: string; extensions: string[] };

// The Fabric package in `directory` (undefined for another package), or null without a package.json.
const fabricPackageIn = (directory: string): FabricPackage | undefined | null => {
  let manifest: { name?: unknown; pi?: { extensions?: unknown } };
  try {
    manifest = JSON.parse(fs.readFileSync(path.join(directory, "package.json"), "utf8")) as typeof manifest;
  } catch {
    return null;
  }
  if (manifest.name !== "pi-fabric") return undefined;
  const extensions = Array.isArray(manifest.pi?.extensions)
    ? manifest.pi.extensions.filter((entry): entry is string => typeof entry === "string")
    : [];
  return { root: directory, extensions };
};

// The Pi Fabric package that holds `file`: the nearest package.json, if it is named "pi-fabric".
const fabricPackageOf = (file: string): FabricPackage | undefined => {
  let directory = path.dirname(file);
  for (let depth = 0; depth < 6; depth++) {
    const found = fabricPackageIn(directory);
    if (found !== null) return found;
    const parent = path.dirname(directory);
    if (parent === directory) return undefined;
    directory = parent;
  }
  return undefined;
};

const canonical = (file: string): string => {
  try {
    return fs.realpathSync(file);
  } catch {
    return path.resolve(file);
  }
};

// The Fabric package root that a Pi extension argument loads: the package directory, one of
// its `pi.extensions` entries, either supported compiled entry, or `src/index.ts`
// (development). Any other file in the
// package, such as the worker's activation-window hook, loads a hook and not Fabric.
const requestedFabricRoot = (requested: string): string | undefined => {
  const directoryPackage = fabricPackageIn(requested);
  if (directoryPackage) return directoryPackage.root;
  const fabric = fabricPackageOf(requested);
  if (!fabric) return undefined;
  const entries = [...fabric.extensions, "dist/index.js", "dist/extension-bootstrap.js", "src/index.ts"].map((entry) => canonical(path.resolve(fabric.root, entry)));
  return entries.includes(requested) ? fabric.root : undefined;
};

/**
 * Suppress registration aliases on this host's event bus, not in a process-global
 * singleton: SDK hosts and reloads must remain independent. Pi emits bus events
 * synchronously; the first alias answers subsequent claims until shutdown.
 */
export const claimFabricRegistration = (
  ownEntry: string,
  pi: Pick<ExtensionAPI, "events" | "on">,
): boolean => {
  const entry = canonical(ownEntry);
  const root = canonical(fabricPackageOf(entry)?.root ?? path.dirname(entry));
  const channel = "pi-fabric:registration-claim:v1";
  const claim = { root, claimed: false };
  pi.events.emit(channel, claim);
  if (claim.claimed) return false;
  const release = pi.events.on(channel, (data: unknown) => {
    if (data && typeof data === "object" && (data as { root?: unknown }).root === root) {
      (data as { claimed: boolean }).claimed = true;
    }
  });
  pi.on("session_shutdown", () => { release(); });
  return true;
};

/**
 * Pi Fabric package roots requested explicitly on the Pi command line (`-e PATH` or
 * `--extension PATH`, the forms Pi's argument parser accepts). A Fabric agent worker starts
 * its child Pi with the parent's Fabric this way, while the child also discovers the
 * profile's Fabric package. Returns canonical package roots; relative paths resolve from `cwd`.
 */
export const explicitFabricRoots = (argv: readonly string[], cwd: string): string[] => {
  const roots = new Set<string>();
  for (let index = 0; index < argv.length - 1; index++) {
    if (!EXTENSION_FLAGS.has(argv[index]!)) continue;
    const root = requestedFabricRoot(canonical(path.resolve(cwd, argv[++index]!)));
    if (root) roots.add(canonical(root));
  }
  return [...roots];
};

/**
 * True when this Fabric copy must stay inert because a different Fabric was requested
 * explicitly on the command line. Two Fabric copies in one Pi collide on `fabric_exec`
 * and Pi refuses to start; during a staged upgrade a worker's child Pi would otherwise
 * load the parent's older Fabric (via `-e`) and the profile's newer one (smarty-dev#266).
 * The explicit choice wins. With no explicit Fabric, or when this copy is the explicit
 * one, nothing changes.
 */
export const yieldsToExplicitFabric = (
  ownEntry: string,
  argv: readonly string[] = process.argv,
  cwd: string = process.cwd(),
): boolean => {
  const explicit = explicitFabricRoots(argv, cwd);
  if (explicit.length === 0) return false;
  const own = fabricPackageOf(canonical(ownEntry))?.root;
  return own !== undefined && !explicit.includes(canonical(own));
};
