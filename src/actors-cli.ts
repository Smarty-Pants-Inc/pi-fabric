import fs from "node:fs";
import path from "node:path";
import { MAIN_STOPPED_AUDIT_REQUIRED, MAIN_STOPPED_EVIDENCE_MAX_BYTES, mainStoppedAudit, removeActorOffline } from "./actors/remove-offline.js";
import { ResidentActorClient } from "./residency/actor-client.js";
import { residentRoot, residentHostId, type ResidentHostConfig, type ResidentHostOwner } from "./residency/protocol.js";
import { residentProcessAlive } from "./residency/process-identity.js";
import { FileLockBusy, lockFile } from "./residency/file-lock.js";

const usage = "Usage: fabric-actors stop|remove --resident <directory-or-prefix> --actor <id-or-name> [--mesh-root <dir>] [--dry-run] [--confirm-dead-root <rootId>] [--main-stopped --evidence <text> | --evidence-file <path>]\n" +
  "       fabric-actors adopt --resident <dead-directory-or-prefix> --actor <id-or-name> --into <live-rootId-or-resident> [--mesh-root <dir>] [--dry-run] [--confirm-dead-root <rootId>]";
const help = `${usage}

When the resident host is dead, remove runs offline and needs --main-stopped (the live resident's own remove
path is unchanged; smarty-dev#8090). The offline removal archives the actor by one rename of its directory into
<residency>/archives/<id>.<time>.<rand>/ (same filesystem only) beside its registry row and audit record;
deleting archives is left to retention (smarty-dev#7916). --main-stopped is an OPERATOR ATTESTATION, not a machine proof: the operator
states that the root's Main process is gone. Chief-of-staff accepted it on 2026-10-09 for the operator of the
dead Main's own fleet, or Light for the #7231 waves; nobody else uses it. The automatic proof is smarty-dev#7956.
It needs --evidence <text> or --evidence-file <path> (read as text, capped at 64 KiB): the Herdr pane/agent
listing and the process check for that Main's session, kept as operatorAttestation. The tool adds its own
observation (every root participant record with its pid, host, release and last-seen time, a /proc check of each
pid on this host, the owner lease state, and the time), and the operator ($USER, PI_FABRIC_AGENT_NAME or the
session id), root id and time, in the removal archive and record. It never overrides a live root lease, a
fresh, reloading or doubtful root participant, or a participant process alive on this host.`;

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
    if (argv.length === 1 && (argv[0] === "--help" || argv[0] === "-h")) { io.out(help + "\n"); return 0; }
    const [action, ...rest] = argv;
    if (action !== "stop" && action !== "remove" && action !== "adopt") throw new Error(usage);
    const values: Record<string, string> = {};
    let dryRun = false;
    const options = ["--resident", "--actor", "--mesh-root", "--confirm-dead-root", ...(action === "adopt" ? ["--into"] : [])];
    // remove: the operator asserts the root's Main process is gone (automatic proof: smarty-dev#7956).
    let mainStopped = false;
    for (let index = 0; index < rest.length; index++) {
      const option = rest[index]!;
      if (option === "--dry-run") { dryRun = true; continue; }
      if (option === "--main-stopped" && !mainStopped) { mainStopped = true; continue; }
      if (![...options, "--evidence", "--evidence-file"].includes(option) || values[option] !== undefined ||
          !rest[index + 1] || rest[index + 1]!.startsWith("--")) throw new Error(usage);
      values[option] = rest[++index]!;
    }
    if (!values["--resident"] || !values["--actor"] || (action === "adopt" && !values["--into"])) throw new Error(usage);
    const evidenceGiven = values["--evidence"] !== undefined || values["--evidence-file"] !== undefined;
    if ((values["--evidence"] !== undefined && values["--evidence-file"] !== undefined) || (evidenceGiven && !mainStopped) ||
        (mainStopped && action !== "remove")) throw new Error(usage);
    if (mainStopped && !evidenceGiven) throw new Error(MAIN_STOPPED_AUDIT_REQUIRED);
    let evidence = values["--evidence"];
    if (values["--evidence-file"] !== undefined) {
      const fd = fs.openSync(values["--evidence-file"], "r");
      try {
        const buffer = Buffer.alloc(MAIN_STOPPED_EVIDENCE_MAX_BYTES);
        evidence = buffer.subarray(0, fs.readSync(fd, buffer, 0, buffer.length, 0)).toString("utf8");
      } finally { fs.closeSync(fd); }
    }
    const meshRoot = path.resolve(values["--mesh-root"] ?? process.env.PI_FABRIC_MESH_ROOT ??
      path.join(process.env.PI_FABRIC_PROJECT_ROOT ?? process.cwd(), ".pi", "fabric", "mesh"));
    const directory = resolveResidentDirectory(values["--resident"], meshRoot);
    if (action === "adopt") {
      const response = await adopt(directory, resolveTargetResidentDirectory(values["--into"]!, meshRoot), values["--actor"], dryRun,
        values["--confirm-dead-root"] !== undefined ? { confirmDeadRoot: values["--confirm-dead-root"] } : {});
      io.out(JSON.stringify({ resident: directory, action, dryRun, ...response }) + "\n");
      return 0;
    }
    const { config, owner: resident } = readResident(directory);
    const owner = resident!; // readResident without allowStopped always reads owner.json
    const confirmation = { ...(values["--confirm-dead-root"] !== undefined ? { confirmDeadRoot: values["--confirm-dead-root"] } : {}),
      ...(mainStopped ? { mainStoppedAudit: mainStoppedAudit(config.rootId, evidence ?? "") } : {}) };
    if (!residentProcessAlive(owner.pid, owner.processStartTime)) {
      // smarty-dev#7817: a dead resident cannot carry out the removal; do it offline under its host.lock fence.
      if (action !== "remove") throw new Error("Root resident host is not live");
      const response = await removeActorOffline(directory, config, values["--actor"], { dryRun, ...confirmation });
      io.out(JSON.stringify({ resident: directory, action, ...response }) + "\n");
      return response.cleaned === false ? 1 : 0;
    }
    // The live resident's own remove path is unchanged here (smarty-dev#8090); the attestation is offline-only.
    if (mainStopped) throw new Error("--main-stopped applies only to an offline removal (dead resident host); the live resident path is smarty-dev#8090");
    // The runtime exchange deliberately unrefs its polling timer. A standalone operator process must
    // stay alive until the acknowledged response is printed: one ref'd timer for the wait's own
    // deadline, cleared when the wait settles. No periodic timer.
    const client = new ResidentActorClient(config.meshRoot, config.rootId);
    const keepAlive = setTimeout(() => {}, client.commandTimeoutMs + 1_000);
    try {
      const response = await client.operatorActor(action,
        values["--actor"], { dryRun, ...(confirmation.confirmDeadRoot !== undefined ? { confirmDeadRoot: confirmation.confirmDeadRoot } : {}) });
      io.out(JSON.stringify({ resident: directory, action, dryRun, ...response }) + "\n");
      return 0;
    } finally { clearTimeout(keepAlive); }
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
  // The client's event-driven wait is unref'd: keep this standalone process alive for at most
  // the two acknowledged exchanges (release, adopt), cleared when they settle. No periodic timer.
  const keepAlive = setTimeout(() => {}, 2 * liveClient.commandTimeoutMs + 1_000);
  try {
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
  } finally { clearTimeout(keepAlive); }
}
