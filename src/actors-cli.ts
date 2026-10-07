import fs from "node:fs";
import path from "node:path";
import { ResidentActorClient } from "./residency/actor-client.js";
import { residentRoot, residentHostId, type ResidentHostConfig, type ResidentHostOwner } from "./residency/protocol.js";
import { residentProcessAlive } from "./residency/process-identity.js";
import { FileLockBusy, lockFile } from "./residency/file-lock.js";

const usage = "Usage: fabric-actors stop|remove --resident <directory-or-prefix> --actor <id-or-name> [--mesh-root <dir>] [--dry-run] [--confirm-dead-root <rootId>]\n" +
  "       fabric-actors adopt --resident <dead-directory-or-prefix> --actor <id-or-name> --into <live-rootId-or-resident> [--mesh-root <dir>] [--dry-run] [--confirm-dead-root <rootId>]";

/** Resolve exactly one resident under the configured residency directory, never the CWD. */
export function resolveResidentDirectory(selector: string, meshRoot: string): string {
  const explicitPath = path.isAbsolute(selector) || /[/\\]/.test(selector);
  if (!explicitPath && !/^[0-9a-f]+$/.test(selector)) {
    throw new Error(`Resident prefix must be hexadecimal: ${selector}`);
  }
  const parent = fs.realpathSync(path.join(meshRoot, "residency"));
  let candidate: string;
  if (explicitPath) {
    candidate = path.resolve(selector);
  } else {
    const matches = fs.readdirSync(parent).filter(name => /^[0-9a-f]+$/.test(name) && name.startsWith(selector) &&
      fs.lstatSync(path.join(parent, name)).isDirectory());
    if (matches.length !== 1) throw new Error(matches.length ? `Ambiguous resident prefix: ${selector}` : `Unknown resident: ${selector}`);
    candidate = path.join(parent, matches[0]!);
  }
  // Check the selector before realpath so a link cannot hide behind its valid target.
  const stat = fs.lstatSync(candidate);
  if (stat.isSymbolicLink()) throw new Error(`Resident path must not be a symlink: ${selector}`);
  if (!stat.isDirectory()) throw new Error(`Resident path is not a directory: ${selector}`);
  const directory = fs.realpathSync(candidate);
  if (path.dirname(directory) !== parent) {
    throw new Error(`Resident path must be a direct child of the configured residency directory: ${selector}`);
  }
  return directory;
}

/** `--into` names a live root id ("session:...") or, like --resident, a resident directory/prefix. */
export function resolveTargetResidentDirectory(selector: string, meshRoot: string): string {
  if (selector.includes(":") && !path.isAbsolute(selector)) {
    const directory = residentRoot(meshRoot, selector);
    if (!fs.existsSync(directory)) throw new Error(`No resident host directory for root ${selector}`);
    return resolveResidentDirectory(directory, meshRoot);
  }
  return resolveResidentDirectory(selector, meshRoot);
}

/** Same-user channel and directory/config/owner identity checks shared by every action. */
function readResident(directory: string, allowStopped = false): { config: ResidentHostConfig; owner?: ResidentHostOwner | undefined } {
  // A cleanly stopped host removes owner.json; only a dead root's host may be stopped.
  const stopped = allowStopped && !fs.existsSync(path.join(directory, "owner.json"));
  // Local OS user boundary, independent of the invoking Fabric session's root.
  for (const file of [directory, path.join(directory, "config.json"), ...(stopped ? [] : [path.join(directory, "owner.json"),
    path.join(directory, "requests"), path.join(directory, "responses")])]) {
    const stat = fs.lstatSync(file);
    if (stat.isSymbolicLink() || (process.getuid && stat.uid !== process.getuid())) {
      throw new Error(`Resident channel is not owned by this OS user: ${file}`);
    }
  }
  const config = JSON.parse(fs.readFileSync(path.join(directory, "config.json"), "utf8")) as ResidentHostConfig;
  const owner = stopped ? undefined : JSON.parse(fs.readFileSync(path.join(directory, "owner.json"), "utf8")) as ResidentHostOwner;
  if (typeof config.rootId !== "string" || typeof config.meshRoot !== "string" ||
      typeof config.residencyRoot !== "string" || (owner !== undefined && owner.hostId !== residentHostId(config.rootId)) ||
      fs.realpathSync(config.residencyRoot) !== directory || fs.realpathSync(residentRoot(config.meshRoot, config.rootId)) !== directory) {
    throw new Error("Resident directory/config/owner identity mismatch");
  }
  return { config, owner };
}

const ownerAlive = (directory: string): boolean => {
  try {
    const owner = JSON.parse(fs.readFileSync(path.join(directory, "owner.json"), "utf8")) as ResidentHostOwner;
    return residentProcessAlive(owner.pid, owner.processStartTime);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    return true; // Unreadable identity is not proof that the host is down.
  }
};

/**
 * smarty-dev#5919: prove a dead root's resident host is not running and keep it from
 * starting while the registry is edited offline: hold its startup claim and host fence.
 */
async function fenceStoppedHost(directory: string): Promise<() => void> {
  const held: number[] = [];
  const release = () => { for (const fd of held.splice(0)) { try { fs.closeSync(fd); } catch { /* already closed */ } } };
  try {
    for (const name of ["host-fence-establish.lock", "host.lock"]) {
      held.push(await lockFile(path.join(directory, name), 0, process.platform === "linux"));
    }
  } catch (error) {
    release();
    if (error instanceof FileLockBusy) throw new Error("Dead root's resident host is running or starting; it holds its host fence");
    throw error;
  }
  if (ownerAlive(directory)) { release(); throw new Error("Dead root's resident host is still running"); }
  return release;
}

export async function main(argv: string[], io: { out: (text: string) => void; err: (text: string) => void } = { out: (text: string) => process.stdout.write(text),
  err: (text: string) => process.stderr.write(text) }): Promise<number> {
  try {
    if (argv.length === 1 && (argv[0] === "--help" || argv[0] === "-h")) { io.out(usage + "\n"); return 0; }
    const [action, ...rest] = argv;
    if (action !== "stop" && action !== "remove" && action !== "adopt") throw new Error(usage);
    const values: Record<string, string> = {};
    let dryRun = false;
    const options = ["--resident", "--actor", "--mesh-root", "--confirm-dead-root", ...(action === "adopt" ? ["--into"] : [])];
    for (let index = 0; index < rest.length; index++) {
      const option = rest[index]!;
      if (option === "--dry-run") { dryRun = true; continue; }
      if (!options.includes(option) || values[option] !== undefined ||
          !rest[index + 1] || rest[index + 1]!.startsWith("--")) throw new Error(usage);
      values[option] = rest[++index]!;
    }
    if (!values["--resident"] || !values["--actor"] || (action === "adopt" && !values["--into"])) throw new Error(usage);
    const meshRoot = path.resolve(values["--mesh-root"] ?? process.env.PI_FABRIC_MESH_ROOT ??
      path.join(process.env.PI_FABRIC_PROJECT_ROOT ?? process.cwd(), ".pi", "fabric", "mesh"));
    const directory = resolveResidentDirectory(values["--resident"], meshRoot);
    const confirmation = values["--confirm-dead-root"] !== undefined ? { confirmDeadRoot: values["--confirm-dead-root"] } : {};
    // The runtime exchange deliberately unrefs its polling timer. A standalone
    // operator process must stay alive until the acknowledged response is printed.
    const keepAlive = setInterval(() => {}, 30_000);
    try {
      if (action === "adopt") {
        const response = await adopt(directory, resolveTargetResidentDirectory(values["--into"]!, meshRoot),
          values["--actor"], dryRun, confirmation);
        io.out(JSON.stringify({ resident: directory, action, dryRun, ...response }) + "\n");
        return 0;
      }
      const { config, owner } = readResident(directory);
      if (!owner || !residentProcessAlive(owner.pid, owner.processStartTime)) throw new Error("Root resident host is not live");
      const response = await new ResidentActorClient(config.meshRoot, config.rootId).operatorActor(action,
        values["--actor"], { dryRun, ...confirmation });
      io.out(JSON.stringify({ resident: directory, action, dryRun, ...response }) + "\n");
      return 0;
    } finally { clearInterval(keepAlive); }
  } catch (error) {
    io.err(`fabric-actors: ${error instanceof Error ? error.message : String(error)}\n`);
    return 1;
  }
}

/**
 * smarty-dev#5919: adopt through BOTH resident request channels. A live dead-root host
 * releases the actor through its own channel (same confirmation and lease veto as
 * stop/remove); a stopped one is fenced offline. The live host then moves custody.
 */
async function adopt(deadDirectory: string, liveDirectory: string, actor: string, dryRun: boolean,
  confirmation: { confirmDeadRoot?: string }): Promise<Record<string, unknown>> {
  if (deadDirectory === liveDirectory) throw new Error("--into must name another root than --resident");
  const dead = readResident(deadDirectory, true), live = readResident(liveDirectory);
  if (path.resolve(dead.config.meshRoot) !== path.resolve(live.config.meshRoot)) throw new Error("Dead and live roots use different meshes");
  if (!live.owner || !residentProcessAlive(live.owner.pid, live.owner.processStartTime)) throw new Error("Adoption target resident host is not live");
  const liveClient = new ResidentActorClient(live.config.meshRoot, live.config.rootId);
  const deadLive = dead.owner !== undefined && residentProcessAlive(dead.owner.pid, dead.owner.processStartTime);
  if (dryRun) {
    const preview = await liveClient.adoptActor(actor, dead.config.rootId, { dryRun, ...confirmation });
    return { into: liveDirectory, deadHost: deadLive ? "live" : "stopped", ...preview };
  }
  let released: Record<string, unknown> | undefined;
  let releaseFence: (() => void) | undefined;
  if (deadLive) {
    const response = await new ResidentActorClient(dead.config.meshRoot, dead.config.rootId).releaseActor(actor, confirmation);
    released = { actor: response.actor?.id, status: response.actor?.status };
    actor = response.actor?.id ?? actor;
  } else {
    releaseFence = await fenceStoppedHost(deadDirectory);
  }
  try {
    const response = await liveClient.adoptActor(actor, dead.config.rootId, confirmation);
    return { into: liveDirectory, deadHost: deadLive ? "live" : "stopped", ...(released ? { released } : {}), ...response };
  } finally { releaseFence?.(); }
}
