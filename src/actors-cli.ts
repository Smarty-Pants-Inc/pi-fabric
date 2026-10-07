import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { ResidentActorClient } from "./residency/actor-client.js";
import { residentRoot, residentHostId, type ResidentHostConfig, type ResidentHostOwner } from "./residency/protocol.js";
import { residentProcessAlive } from "./residency/process-identity.js";

const usage = "Usage: fabric-actors stop|remove --resident <directory-or-prefix> --actor <id-or-name> [--mesh-root <dir>] [--dry-run] [--confirm-dead-root <rootId>]";

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

export interface ResidentChannelOwnerProbe {
  platform: NodeJS.Platform;
  getuid?: () => number;
  /** win32 only: owner SID of each path followed by the current user's SID. */
  windowsOwnerSids?: (files: string[]) => { owners: string[]; user: string };
}

const windowsSidScript = [
  "$ErrorActionPreference = 'Stop'",
  "$sid = [System.Security.Principal.SecurityIdentifier]",
  // No double quotes: Windows argv quoting of " is unreliable for powershell.exe -Command.
  "$paths = $env:FABRIC_ACTORS_OWNER_PATHS -split [char]10",
  "foreach ($p in $paths) { [Console]::Out.WriteLine((Get-Acl -LiteralPath $p).GetOwner($sid).Value) }",
  "[Console]::Out.WriteLine([System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value)",
].join("; ");

/** Query owner SIDs with a fixed argv; the paths travel in the environment, never the command line. */
export function windowsOwnerSids(files: string[]): { owners: string[]; user: string } {
  if (files.some(file => /[\r\n]/.test(file))) throw new Error("path contains a line break");
  const shell = path.win32.join(process.env.SystemRoot ?? "C:\\Windows", "System32", "WindowsPowerShell", "v1.0", "powershell.exe");
  const output = execFileSync(shell, ["-NoProfile", "-NonInteractive", "-Command", windowsSidScript], {
    encoding: "utf8", windowsHide: true, timeout: 30_000, stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, FABRIC_ACTORS_OWNER_PATHS: files.join("\n") },
  });
  const lines = output.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
  if (lines.length !== files.length + 1) throw new Error("unexpected owner query output");
  return { owners: lines.slice(0, files.length), user: lines[files.length]! };
}

const defaultOwnerProbe = (): ResidentChannelOwnerProbe => ({ platform: process.platform,
  ...(process.getuid ? { getuid: () => process.getuid!() } : {}), windowsOwnerSids });
const sidPattern = /^S-1-\d+(-\d+)+$/;

/** Fail closed unless every channel path is a non-symlink owned by the invoking OS user. */
export function assertResidentChannelOwned(files: string[], probe: ResidentChannelOwnerProbe = defaultOwnerProbe()): void {
  const stats = files.map(file => {
    const stat = fs.lstatSync(file);
    if (stat.isSymbolicLink()) throw new Error(`Resident channel is not owned by this OS user: ${file}`);
    return stat;
  });
  if (probe.platform === "win32") {
    // Node reports uid 0 on Windows, so stat cannot prove ownership; compare owner SIDs instead.
    let result: { owners: string[]; user: string };
    try {
      if (!probe.windowsOwnerSids) throw new Error("no owner SID query is available");
      result = probe.windowsOwnerSids(files);
    } catch (error) {
      throw new Error(`Cannot verify that the resident channel is owned by this Windows user (${error instanceof Error ? error.message : String(error)}); refusing stop/remove`);
    }
    if (!sidPattern.test(result.user) || result.owners.length !== files.length) {
      throw new Error("Cannot verify that the resident channel is owned by this Windows user (malformed SID query result); refusing stop/remove");
    }
    files.forEach((file, index) => {
      const owner = result.owners[index]!;
      if (!sidPattern.test(owner) || owner !== result.user) throw new Error(`Resident channel is not owned by this OS user: ${file}`);
    });
    return;
  }
  if (!probe.getuid) throw new Error("Cannot verify that the resident channel is owned by this OS user (no uid available); refusing stop/remove");
  const uid = probe.getuid();
  files.forEach((file, index) => {
    if (stats[index]!.uid !== uid) throw new Error(`Resident channel is not owned by this OS user: ${file}`);
  });
}

export async function main(argv: string[], io: { out: (text: string) => void; err: (text: string) => void } = { out: (text: string) => process.stdout.write(text),
  err: (text: string) => process.stderr.write(text) }, ownerProbe: ResidentChannelOwnerProbe = defaultOwnerProbe()): Promise<number> {
  try {
    if (argv.length === 1 && (argv[0] === "--help" || argv[0] === "-h")) { io.out(usage + "\n"); return 0; }
    const [action, ...rest] = argv;
    if (action !== "stop" && action !== "remove") throw new Error(usage);
    const values: Record<string, string> = {};
    let dryRun = false;
    for (let index = 0; index < rest.length; index++) {
      const option = rest[index]!;
      if (option === "--dry-run") { dryRun = true; continue; }
      if (!["--resident", "--actor", "--mesh-root", "--confirm-dead-root"].includes(option) || values[option] !== undefined ||
          !rest[index + 1] || rest[index + 1]!.startsWith("--")) throw new Error(usage);
      values[option] = rest[++index]!;
    }
    if (!values["--resident"] || !values["--actor"]) throw new Error(usage);
    const meshRoot = path.resolve(values["--mesh-root"] ?? process.env.PI_FABRIC_MESH_ROOT ??
      path.join(process.env.PI_FABRIC_PROJECT_ROOT ?? process.cwd(), ".pi", "fabric", "mesh"));
    const directory = resolveResidentDirectory(values["--resident"], meshRoot);
    // Local OS user boundary, independent of the invoking Fabric session's root.
    assertResidentChannelOwned([directory, path.join(directory, "config.json"), path.join(directory, "owner.json"),
      path.join(directory, "requests"), path.join(directory, "responses")], ownerProbe);
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
        values["--actor"], { dryRun, ...(values["--confirm-dead-root"] !== undefined ? { confirmDeadRoot: values["--confirm-dead-root"] } : {}) });
      io.out(JSON.stringify({ resident: directory, action, dryRun, ...response }) + "\n");
      return 0;
    } finally { clearInterval(keepAlive); }
  } catch (error) {
    io.err(`fabric-actors: ${error instanceof Error ? error.message : String(error)}\n`);
    return 1;
  }
}
