// fabric-participants (smarty-dev#395): the mesh's participant directory as JSON, read-only.
//
//   fabric-participants [--json] [--mesh DIR] [--include-stale] [--kind root|agent|actor]...
//
// It lists through ParticipantDirectory.list(), the code agents.members uses, so readers such as
// knowledge-live need no parser of their own for the mesh's state.json. Contract: docs/participants-cli.md.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { MeshStore } from "./mesh/store.js";
import { ParticipantDirectory } from "./topology/participant-directory.js";
import type { FabricParticipantInfo, FabricParticipantKind } from "./topology/types.js";

// Fabric's defaults for mesh.maxEventBytes and mesh.maxReadEvents (src/config.ts).
const MAX_EVENT_BYTES = 256 * 1024;
const MAX_READ_EVENTS = 500;
const KINDS: readonly FabricParticipantKind[] = ["root", "agent", "actor"];

const USAGE = `usage: fabric-participants [--json] [--mesh DIR] [--include-stale] [--kind root|agent|actor]...`;

/** An error the CLI reports by name on stderr, with exit code 2. */
class ParticipantsCliError extends Error {
  constructor(readonly code: "FABRIC_USAGE" | "FABRIC_MESH_MISSING", message: string) {
    super(message);
  }
}

const readOnly = (): Error => new Error("fabric-participants opens the mesh read-only");

/**
 * A mesh store that refuses every write. The CLI never starts the directory's heartbeat, and the
 * only write a listing can attempt is the mirror-refusal event, which must not happen here.
 */
class ReadOnlyMeshStore extends MeshStore {
  override async publish(): Promise<never> { throw readOnly(); }
  override async put(): Promise<never> { throw readOnly(); }
  override async delete(): Promise<never> { throw readOnly(); }
  override async writeBatch(): Promise<never> { throw readOnly(); }
  override async confirmWritable(): Promise<never> { throw readOnly(); }
}

const readJson = (file: string): Record<string, unknown> => {
  try {
    const value: unknown = JSON.parse(fs.readFileSync(file, "utf8"));
    return typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {};
  } catch {
    return {};
  }
};

const configuredRoot = (config: Record<string, unknown>): string | undefined => {
  const mesh = config.mesh;
  const root = typeof mesh === "object" && mesh !== null ? (mesh as Record<string, unknown>).root : undefined;
  return typeof root === "string" && root.trim() ? root.trim() : undefined;
};

/**
 * The mesh root a Pi started in cwd would use (src/fabric-runtime-state.ts): PI_FABRIC_MESH_ROOT,
 * else mesh.root from the project's .pi/fabric.json over the agent dir's fabric.json, relative to
 * the project root, else <project>/.pi/fabric/mesh. It reads the files as they are.
 * ponytail: loadFabricConfig would also migrate (rewrite) an old config file, so it is not used.
 */
export const resolveMeshRoot = (env: NodeJS.ProcessEnv = process.env, cwd = process.cwd()): string => {
  if (env.PI_FABRIC_MESH_ROOT) return env.PI_FABRIC_MESH_ROOT;
  const projectRoot = env.PI_FABRIC_PROJECT_ROOT ?? cwd;
  const agentDir = env.PI_CODING_AGENT_DIR
    ? env.PI_CODING_AGENT_DIR.replace(/^~(?=$|\/)/, os.homedir())
    : path.join(os.homedir(), ".pi", "agent");
  const configured = configuredRoot(readJson(path.join(projectRoot, ".pi", "fabric.json"))) ??
    configuredRoot(readJson(path.join(agentDir, "fabric.json")));
  return configured ? path.resolve(projectRoot, configured) : path.join(projectRoot, ".pi", "fabric", "mesh");
};

interface ParticipantsOptions {
  mesh?: string;
  includeStale?: boolean;
  kinds?: FabricParticipantKind[];
}

const parseArgs = (argv: string[]): ParticipantsOptions => {
  const options: ParticipantsOptions = {};
  // "participants" is accepted as a leading subcommand: `pi-fabric participants --json`.
  const args = argv[0] === "participants" ? argv.slice(1) : argv;
  for (let index = 0; index < args.length; index += 1) {
    const flag = args[index]!;
    if (flag === "--json") continue;                           // JSON is the only output format
    if (flag === "--include-stale") {
      options.includeStale = true;
      continue;
    }
    const value = args[index + 1];
    if ((flag === "--mesh" || flag === "--kind") && value !== undefined && !value.startsWith("--")) {
      index += 1;
      if (flag === "--mesh") {
        options.mesh = value;
        continue;
      }
      for (const kind of value.split(",")) {
        if (!KINDS.includes(kind as FabricParticipantKind)) {
          throw new ParticipantsCliError("FABRIC_USAGE", `Unknown --kind ${kind}; expected ${KINDS.join(", ")}\n${USAGE}`);
        }
        (options.kinds ??= []).push(kind as FabricParticipantKind);
      }
      continue;
    }
    throw new ParticipantsCliError("FABRIC_USAGE", `Bad argument: ${flag}\n${USAGE}`);
  }
  return options;
};

/** The participants on a mesh root, as agents.members lists them for a reader outside the mesh. */
const listParticipants = (options: ParticipantsOptions = {}): FabricParticipantInfo[] => {
  const root = path.resolve(options.mesh ?? resolveMeshRoot());
  // MeshStore creates its root: check first, so a wrong path is an error and not a new empty mesh.
  if (!fs.statSync(root, { throwIfNoEntry: false })?.isDirectory()) {
    throw new ParticipantsCliError("FABRIC_MESH_MISSING", `Fabric mesh directory not found: ${root}`);
  }
  const id = `participants-cli:${process.pid}:${Date.now()}`;
  // An identity no participant has: this reader is not a host, so every entry lists local: false.
  const directory = new ParticipantDirectory(new ReadOnlyMeshStore(root, MAX_EVENT_BYTES, MAX_READ_EVENTS), {
    enabled: true,
    hostId: id,
    rootId: id,
    identity: { id, name: "fabric-participants", kind: "main" },
    reapDeadHosts: false,
  });
  // Scope project: the whole mesh without the reader's own synthetic self entry.
  return directory.list({
    scope: "project",
    fresh: true,
    ...(options.includeStale ? { includeStale: true } : {}),
    ...(options.kinds ? { kinds: options.kinds } : {}),
  });
};

export const main = async (
  argv: string[],
  out: (text: string) => void = (text) => process.stdout.write(text),
  err: (text: string) => void = (text) => process.stderr.write(text),
): Promise<number> => {
  try {
    out(`${JSON.stringify(listParticipants(parseArgs(argv)), null, 2)}\n`);
    return 0;
  } catch (error) {
    if (error instanceof ParticipantsCliError) {
      err(`fabric-participants: ${error.code}: ${error.message}\n`);
      return 2;
    }
    throw error;
  }
};
