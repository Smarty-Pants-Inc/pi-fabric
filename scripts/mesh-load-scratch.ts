import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { resolveMeshRoot } from "../src/participants-cli.js";
import { resolveAgentDir, resolveMeshDirectory, resolveProjectRoot } from "../src/core/agent-dir.js";

export const SCRATCH_MARKER = ".mesh-load-scratch";

/** Root refusals are usage errors, never a reason to try opening a mesh backend. */
export class MeshLoadRootError extends Error {
  constructor(readonly code: "MESH_LOAD_LIVE_ROOT" | "MESH_LOAD_NOT_SCRATCH" | "MESH_LOAD_ROOT_UNSAFE", message: string) {
    super(message);
    this.name = "MeshLoadRootError";
  }
}

const expandHome = (directory: string): string => directory.replace(/^~(?=$|[\\/])/, os.homedir());

/** Resolve existing symlink components even when the leaf does not exist yet. Fail closed on other errors. */
const canonicalPath = (directory: string): string => {
  let current = path.resolve(expandHome(directory));
  const missing: string[] = [];
  for (;;) {
    try { return path.join(fs.realpathSync(current), ...missing); }
    catch (error) {
      // A dangling symlink is not a missing directory we may safely append to.
      if ((error as NodeJS.ErrnoException).code !== "ENOENT" || fs.lstatSync(current, { throwIfNoEntry: false })) throw error;
      const parent = path.dirname(current);
      if (parent === current) throw error;
      missing.unshift(path.basename(current));
      current = parent;
    }
  }
};

const contains = (parent: string, child: string): boolean => {
  const relative = path.relative(parent, child);
  return relative === "" || (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative));
};

const liveRoots = (): string[] => {
  const cwd = process.cwd();
  const project = resolveProjectRoot(cwd);
  const agent = resolveAgentDir();
  // Use the runtime's resolver for every candidate, not just the winning config/override.
  // Protect fleet DIR/mesh.dir aliases too, without migrating config or opening a backend.
  const withoutOverride = { ...process.env, PI_FABRIC_MESH_ROOT: undefined };
  const roots = [resolveMeshRoot(), resolveMeshDirectory(undefined, cwd),
    resolveMeshDirectory(undefined, cwd, withoutOverride),
    path.join(os.homedir(), ".local", "share", "smarty-dev", "fabric-mesh")];
  if (process.env.PI_FABRIC_MESH_DIR?.trim()) roots.push(process.env.PI_FABRIC_MESH_DIR.trim());
  const files = new Set([path.join(agent, "fabric.json"), path.join(project, ".pi", "fabric.json"), path.join(cwd, ".pi", "fabric.json")]);
  for (const file of files) {
    let document: { mesh?: { dir?: unknown; root?: unknown } };
    try { document = JSON.parse(fs.readFileSync(file, "utf8")) as typeof document; }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") continue;
      throw error;
    }
    for (const value of [document?.mesh?.dir, document?.mesh?.root]) {
      if (typeof value === "string" && value.trim()) roots.push(resolveMeshDirectory(expandHome(value.trim()), cwd, withoutOverride));
    }
  }
  return roots;
};

const assertNotLiveRoot = (root: string): string => {
  try {
    const lexical = path.resolve(expandHome(root));
    const canonical = canonicalPath(root);
    for (const live of liveRoots()) {
      const aliases = [path.resolve(expandHome(live)), canonicalPath(live)];
      for (const candidate of [lexical, canonical]) for (const protectedRoot of aliases) {
        if (contains(candidate, protectedRoot) || contains(protectedRoot, candidate)) {
          throw new MeshLoadRootError("MESH_LOAD_LIVE_ROOT", `refusing root ${root}: overlaps live Fabric mesh ${live}`);
        }
      }
    }
    return canonical;
  } catch (error) {
    if (error instanceof MeshLoadRootError) throw error;
    throw new MeshLoadRootError("MESH_LOAD_ROOT_UNSAFE", `cannot safely resolve root ${root}: ${error instanceof Error ? error.message : String(error)}`);
  }
};

/** The only creation path: a private mkdtemp under os.tmpdir(), never an arbitrary --root. */
export const createScratchRoot = (): string => {
  // Do not even create a directory if TMPDIR itself is inside a protected mesh.
  assertNotLiveRoot(path.join(os.tmpdir(), "fabric-mesh-load-"));
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-mesh-load-"));
  const canonical = assertNotLiveRoot(root);
  fs.writeFileSync(path.join(root, SCRATCH_MARKER), JSON.stringify({ format: "mesh-load-scratch/1", root: canonical }) + "\n", { flag: "wx", mode: 0o600 });
  return canonical;
};

/** Reusing a scratch root requires the regular, root-bound marker this script writes on creation. */
export const requireScratchRoot = (root: string): string => {
  const canonical = assertNotLiveRoot(root);
  try {
    if (!fs.statSync(canonical).isDirectory()) throw new Error("not a directory");
    const marker = path.join(canonical, SCRATCH_MARKER);
    if (!fs.lstatSync(marker).isFile()) throw new Error("marker is not a regular file");
    const document = JSON.parse(fs.readFileSync(marker, "utf8")) as { format?: unknown; root?: unknown };
    if (document?.format !== "mesh-load-scratch/1" || document.root !== canonical) throw new Error("invalid or copied marker");
    return canonical;
  } catch (error) {
    throw new MeshLoadRootError("MESH_LOAD_NOT_SCRATCH", `refusing root ${root}: requires a script-created ${SCRATCH_MARKER} (${error instanceof Error ? error.message : String(error)})`);
  }
};
