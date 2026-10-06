import fs from "node:fs";
import path from "node:path";
import { ResidentActorClient } from "./residency/actor-client.js";
import { residentRoot, residentHostId, type ResidentHostConfig, type ResidentHostOwner } from "./residency/protocol.js";
import { residentProcessAlive } from "./residency/process-identity.js";

const usage = "Usage: fabric-actors stop|remove --resident <directory-or-prefix> --actor <id-or-name> [--mesh-root <dir>] [--dry-run] [--force-live]";

/** Resolve exactly one resident directory; never choose the first ambiguous match. */
export function resolveResidentDirectory(selector: string, meshRoot: string): string {
  const absolute = path.resolve(selector);
  if (fs.existsSync(absolute)) return fs.realpathSync(absolute);
  const parent = selector.includes(path.sep) ? path.dirname(absolute) : path.join(meshRoot, "residency");
  const prefix = path.basename(selector);
  const matches = fs.readdirSync(parent).filter(name => name.startsWith(prefix) &&
    fs.lstatSync(path.join(parent, name)).isDirectory());
  if (matches.length !== 1) throw new Error(matches.length ? `Ambiguous resident prefix: ${selector}` : `Unknown resident: ${selector}`);
  return fs.realpathSync(path.join(parent, matches[0]!));
}

export async function main(argv: string[], io: { out: (text: string) => void; err: (text: string) => void } = { out: (text: string) => process.stdout.write(text),
  err: (text: string) => process.stderr.write(text) }): Promise<number> {
  try {
    if (argv.length === 1 && (argv[0] === "--help" || argv[0] === "-h")) { io.out(usage + "\n"); return 0; }
    const [action, ...rest] = argv;
    if (action !== "stop" && action !== "remove") throw new Error(usage);
    const values: Record<string, string> = {};
    let dryRun = false, forceLive = false;
    for (let index = 0; index < rest.length; index++) {
      const option = rest[index]!;
      if (option === "--dry-run") { dryRun = true; continue; }
      if (option === "--force-live") { forceLive = true; continue; }
      if (!["--resident", "--actor", "--mesh-root"].includes(option) || values[option] !== undefined ||
          !rest[index + 1] || rest[index + 1]!.startsWith("--")) throw new Error(usage);
      values[option] = rest[++index]!;
    }
    if (!values["--resident"] || !values["--actor"]) throw new Error(usage);
    const meshRoot = path.resolve(values["--mesh-root"] ?? process.env.PI_FABRIC_MESH_ROOT ??
      path.join(process.env.PI_FABRIC_PROJECT_ROOT ?? process.cwd(), ".pi", "fabric", "mesh"));
    const directory = resolveResidentDirectory(values["--resident"], meshRoot);
    // Local OS user boundary, independent of the invoking Fabric session's root.
    for (const file of [directory, path.join(directory, "config.json"), path.join(directory, "owner.json"),
      path.join(directory, "requests"), path.join(directory, "responses")]) {
      const stat = fs.lstatSync(file);
      if (stat.isSymbolicLink() || (process.getuid && stat.uid !== process.getuid())) {
        throw new Error(`Resident channel is not owned by this OS user: ${file}`);
      }
    }
    const config = JSON.parse(fs.readFileSync(path.join(directory, "config.json"), "utf8")) as ResidentHostConfig;
    const owner = JSON.parse(fs.readFileSync(path.join(directory, "owner.json"), "utf8")) as ResidentHostOwner;
    if (typeof config.rootId !== "string" || typeof config.meshRoot !== "string" ||
        typeof config.residencyRoot !== "string" || owner.hostId !== residentHostId(config.rootId) ||
        fs.realpathSync(config.residencyRoot) !== directory || fs.realpathSync(residentRoot(config.meshRoot, config.rootId)) !== directory) {
      throw new Error("Resident directory/config/owner identity mismatch");
    }
    if (!residentProcessAlive(owner.pid, owner.processStartTime)) throw new Error("Root resident host is not live");
    // The runtime exchange deliberately unrefs its polling timer. A standalone
    // operator process must stay alive until the acknowledged response is printed.
    const keepAlive = setInterval(() => {}, 30_000);
    try {
      const response = await new ResidentActorClient(config.meshRoot, config.rootId).operatorActor(action,
        values["--actor"], { dryRun, forceLive });
      io.out(JSON.stringify({ resident: directory, action, dryRun, forceLive, ...response }) + "\n");
      return 0;
    } finally { clearInterval(keepAlive); }
  } catch (error) {
    io.err(`fabric-actors: ${error instanceof Error ? error.message : String(error)}\n`);
    return 1;
  }
}
