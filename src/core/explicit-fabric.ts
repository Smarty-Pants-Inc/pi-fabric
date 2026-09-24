import fs from "node:fs";
import path from "node:path";

const EXTENSION_FLAGS = new Set(["-e", "--extension"]);

// The package root (directory holding a package.json named "pi-fabric") of a file, if any.
const fabricPackageRoot = (file: string): string | undefined => {
  let directory = path.dirname(file);
  for (let depth = 0; depth < 6; depth++) {
    try {
      const manifest = JSON.parse(fs.readFileSync(path.join(directory, "package.json"), "utf8")) as { name?: unknown };
      if (manifest.name === "pi-fabric") return directory;
      return undefined;
    } catch {
      const parent = path.dirname(directory);
      if (parent === directory) return undefined;
      directory = parent;
    }
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

/**
 * Pi Fabric package roots requested explicitly on the Pi command line (`-e PATH`,
 * `--extension PATH`, or `--extension=PATH`). A Fabric agent worker starts its child Pi
 * with the parent's Fabric this way, while the child also discovers the profile's Fabric
 * package. Returns canonical package roots; relative paths resolve from `cwd`.
 */
export const explicitFabricRoots = (argv: readonly string[], cwd: string): string[] => {
  const requested: string[] = [];
  for (let index = 0; index < argv.length; index++) {
    const argument = argv[index]!;
    if (EXTENSION_FLAGS.has(argument) && index + 1 < argv.length) requested.push(argv[++index]!);
    else if (argument.startsWith("--extension=")) requested.push(argument.slice("--extension=".length));
  }
  const roots = new Set<string>();
  for (const entry of requested) {
    const root = fabricPackageRoot(canonical(path.resolve(cwd, entry)));
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
  const own = fabricPackageRoot(canonical(ownEntry));
  return own !== undefined && !explicit.includes(canonical(own));
};
